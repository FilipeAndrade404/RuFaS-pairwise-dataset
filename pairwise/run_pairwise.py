#!/usr/bin/env python3
"""
Executa automaticamente as 154 simulações pairwise do RuFaS descritas em
"Geração de Dataset de Produção de Leite no RuFaS" (IFG - Câmpus Inhumas).

Etapas (subcomandos):
    verify       Confere a matriz (154 cenários, 834/834 pares, Tabela 4).
    generate     Gera os arquivos de entrada de cada cenário a partir do example_freestall.
    run          Executa os cenários (em paralelo, retomável, com diagnóstico de falhas).
    consolidate  Junta as saídas das execuções concluídas em dois CSVs (protocolo + dataset).
    all          verify + generate + run + consolidate.

Uso (a partir da raiz do repositório RuFaS):
    python pairwise/run_pairwise.py all --workers 4
    python pairwise/run_pairwise.py run --only PW001,PW002 --force
    python pairwise/run_pairwise.py all --max-days 30 --only PW005   # teste rápido

Todos os parâmetros fora da seleção da Seção 3 permanecem com os valores do example_freestall.
"""

from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import itertools
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------------------
# Caminhos
# --------------------------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
PAIRWISE_DIR = REPO_ROOT / "pairwise"
SCENARIOS_CSV = PAIRWISE_DIR / "scenarios.csv"
FILTERS_DIR = PAIRWISE_DIR / "output_filters"
WORK_DIR = PAIRWISE_DIR / "work"
DATASET_DIR = PAIRWISE_DIR / "dataset"

BASE_METADATA = REPO_ROOT / "input/metadata/example_freestall_dairy_metadata.json"
BASE_TASK = REPO_ROOT / "input/data/tasks/example_freestall_task.json"

# --------------------------------------------------------------------------------------------------
# Definições da metodologia
# --------------------------------------------------------------------------------------------------
# Tabela 3 - perfis de paridade (1ª, 2ª, 3ª ou mais)
PARITY_PROFILES: dict[str, tuple[float, float, float]] = {
    "Menos primiparas": (0.324, 0.278, 0.398),
    "Base": (0.36, 0.26, 0.38),
    "Mais primiparas": (0.396, 0.242, 0.362),
}

# Tabela 2 - FIPS -> região (apenas para o arquivo de protocolo; o RuFaS deriva a região do FIPS)
FIPS_REGIONS: dict[int, str] = {
    1001: "Southeast",
    4001: "Mountain",
    5001: "Delta",
    6001: "West Coast",
    9001: "Northeast",
    11001: "None",
    17001: "Corn Belt",
    20001: "Northern Plains",
    21001: "Appalachian",
    26001: "Lake",
    36001: "New York",
    40001: "Southern Plains",
    42001: "Pennsylvania",
    55025: "Wisconsin",
}

# Níveis esperados de cada fator (Seção 3)
EXPECTED_LEVELS: dict[str, set] = {
    "fips": set(FIPS_REGIONS),
    "end_year": set(range(2006, 2017)),
    "cow_num": {10, 50, 100},
    "milkings_per_day": {2, 3},
    "parity_profile": set(PARITY_PROFILES),
    "milking_cow_pct": set(range(10, 101, 10)),
    "prod_cut_kg": {0.0, 0.25, 0.5},
}
EXPECTED_SCENARIOS = 154
EXPECTED_PAIRS = 834
SIMULATION_YEARS = 5
RANDOM_SEED = 42

PROTOCOL_COLUMNS = [
    "SCENARIO_ID",
    "INITIAL_COW_NUM",
    "SIMULATION_PERIOD",
    "REGION",
    "COW_TIMES_MILKED_PER_DAY",
    "INITIAL_PARITY_DISTRIBUTION",
    "INITIAL_MILKING_COW_FRACTION",
    "FEEDING_SCENARIO_PROD_CUT",
]
DATASET_COLUMNS = [
    "SCENARIO_ID",
    "COW_ID",
    "PEN_ID",
    "IS_MILKING",
    "PARITY",
    "DAYS_IN_MILK",
    "MILK_PRODUCED",
    "MILK_LACTOSE",
    "MILK_PROTEIN",
    "MILK_FAT",
    "FEED_EFFICIENCY",
    "MILKING_DATE",
    "DAILY_METHANE_EMISSION",
    "DAILY_MILK_PRODUCED",
    "DATE",
]
MILK_VARIABLE_SUFFIX = "milk_data_at_milk_update"


# --------------------------------------------------------------------------------------------------
# Leitura e verificação da matriz
# --------------------------------------------------------------------------------------------------
def load_scenarios() -> list[dict[str, Any]]:
    scenarios = []
    with open(SCENARIOS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            scenarios.append(
                {
                    "scenario_id": row["scenario_id"],
                    "cow_num": int(row["cow_num"]),
                    "start_year": int(row["start_year"]),
                    "end_year": int(row["end_year"]),
                    "fips": int(row["fips"]),
                    "milkings_per_day": int(row["milkings_per_day"]),
                    "parity_profile": row["parity_profile"],
                    "milking_cow_pct": int(row["milking_cow_pct"]),
                    "prod_cut_kg": round(float(row["prod_cut_kg"]), 2),
                }
            )
    return scenarios


def verify(scenarios: list[dict[str, Any]]) -> bool:
    ok = True

    def fail(msg: str) -> None:
        nonlocal ok
        ok = False
        print(f"  [ERRO] {msg}")

    print(f"Cenários: {len(scenarios)} (esperado {EXPECTED_SCENARIOS})")
    if len(scenarios) != EXPECTED_SCENARIOS:
        fail("número de cenários diferente do esperado")
    ids = [s["scenario_id"] for s in scenarios]
    if len(set(ids)) != len(ids):
        fail("IDs de cenário duplicados")

    for s in scenarios:
        if s["end_year"] - s["start_year"] + 1 != SIMULATION_YEARS:
            fail(f"{s['scenario_id']}: período não tem {SIMULATION_YEARS} anos")
        for factor, levels in EXPECTED_LEVELS.items():
            if s[factor] not in levels:
                fail(f"{s['scenario_id']}: {factor}={s[factor]} fora dos níveis esperados")

    total_space = 1
    for levels in EXPECTED_LEVELS.values():
        total_space *= len(levels)
    print(f"Grade completa: {total_space} configurações")

    covered_total = expected_total = 0
    print(f"{'Fator A':<18}{'Fator B':<18}{'Cobertos':>9}{'Esperados':>10}")
    for a, b in itertools.combinations(EXPECTED_LEVELS, 2):
        expected = len(EXPECTED_LEVELS[a]) * len(EXPECTED_LEVELS[b])
        covered = len({(s[a], s[b]) for s in scenarios})
        covered_total += covered
        expected_total += expected
        print(f"{a:<18}{b:<18}{covered:>9}{expected:>10}")
        if covered != expected:
            fail(f"par {a} x {b} incompleto")
    print(f"Pares cobertos: {covered_total}/{expected_total} (esperado {EXPECTED_PAIRS})")
    if expected_total != EXPECTED_PAIRS or covered_total != EXPECTED_PAIRS:
        fail("cobertura pairwise diferente de 834/834")
    print("Matriz OK" if ok else "Matriz com problemas")
    return ok


# --------------------------------------------------------------------------------------------------
# Geração das entradas
# --------------------------------------------------------------------------------------------------
def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def rel(path: Path) -> str:
    """Caminho relativo à raiz do repositório, com barras normais (o RuFaS roda com cwd = raiz)."""
    return path.resolve().relative_to(REPO_ROOT).as_posix()


def parity_fractions(profile: str, base_fractions: dict[str, float]) -> dict[str, float]:
    """
    Converte o perfil (1ª, 2ª, 3ª ou mais) nas 5 chaves do RuFaS. A fatia "3ª ou mais" é repartida
    entre as chaves 3, 4 e 5 na mesma proporção do example_freestall (0,18 : 0,11 : 0,09).
    O RuFaS usa os parâmetros de curva da 3ª paridade para a 4ª em diante, então a curva não muda.
    """
    p1, p2, p3plus = PARITY_PROFILES[profile]
    base_3plus = sum(base_fractions[k] for k in ("3", "4", "5"))
    f3 = round(p3plus * base_fractions["3"] / base_3plus, 6)
    f4 = round(p3plus * base_fractions["4"] / base_3plus, 6)
    f5 = round(1.0 - p1 - p2 - f3 - f4, 6)
    return {"1": p1, "2": p2, "3": f3, "4": f4, "5": f5}


def last_day_of_year(year: int) -> int:
    return dt.date(year, 12, 31).timetuple().tm_yday


def scenario_dir(scenario_id: str) -> Path:
    return WORK_DIR / scenario_id


def generate_scenario(s: dict[str, Any], max_days: int | None) -> None:
    sid = s["scenario_id"]
    sdir = scenario_dir(sid)
    inputs = sdir / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)

    metadata = read_json(BASE_METADATA)
    files = metadata["files"]

    # --- config: período e FIPS --------------------------------------------------------------
    config = read_json(REPO_ROOT / files["config"]["path"])
    config["start_date"] = f"{s['start_year']}:1"
    end_year, end_day = s["end_year"], last_day_of_year(s["end_year"])
    if max_days:  # apenas para testes rápidos
        end = dt.date(s["start_year"], 1, 1) + dt.timedelta(days=max_days - 1)
        end_year, end_day = end.year, end.timetuple().tm_yday
    config["end_date"] = f"{end_year}:{end_day}"
    config["FIPS_county_code"] = s["fips"]
    write_json(inputs / "config.json", config)
    files["config"]["path"] = rel(inputs / "config.json")

    # --- animal: rebanho, paridade, lactação, ordenhas ---------------------------------------
    animal = read_json(REPO_ROOT / files["animal"]["path"])
    herd = animal["herd_information"]
    base_herd_num = herd["herd_num"]
    cow_num = s["cow_num"]
    herd["cow_num"] = cow_num
    herd["herd_num"] = cow_num  # tamanho do rebanho a ser mantido
    # limites de compra/venda mantêm a mesma proporção do exemplo (101 e 95 para 100 vacas)
    herd["herd_size_sell_threshold"] = cow_num + (herd["herd_size_sell_threshold"] - base_herd_num)
    herd["herd_size_buy_threshold"] = int(cow_num * herd["herd_size_buy_threshold"] / base_herd_num)
    herd["parity_fractions"] = parity_fractions(s["parity_profile"], herd["parity_fractions"])
    herd["milking_cow_fraction"] = s["milking_cow_pct"] / 100
    animal["animal_config"]["management_decisions"]["cow_times_milked_per_day"] = s["milkings_per_day"]
    write_json(inputs / "animal.json", animal)
    files["animal"]["path"] = rel(inputs / "animal.json")

    # --- feed: corte máximo de produção admitido na formulação da dieta ----------------------
    feed = read_json(REPO_ROOT / files["feed"]["path"])
    feed["ration_formulation_parameters"]["milk_reduction_maximum"] = s["prod_cut_kg"]
    write_json(inputs / "feed.json", feed)
    files["feed"]["path"] = rel(inputs / "feed.json")

    write_json(inputs / "metadata.json", metadata)

    # --- task + task manager -----------------------------------------------------------------
    base_task = read_json(BASE_TASK)["tasks"][0]
    task = copy.deepcopy(base_task)
    task.update(
        {
            "task_type": "SIMULATION_SINGLE_RUN",
            "metadata_file_path": rel(inputs / "metadata.json"),
            "output_prefix": sid,
            "log_verbosity": "errors",
            "random_seeds": [RANDOM_SEED],
            "exclude_info_maps": True,
            "filters_directory": rel(FILTERS_DIR) + "/",
            "csv_output_directory": rel(sdir / "output/CSVs") + "/",
            "json_output_directory": rel(sdir / "output/JSONs") + "/",
            "report_directory": rel(sdir / "output/reports") + "/",
            "graphics_directory": rel(sdir / "output/graphics") + "/",
            "logs_directory": rel(sdir / "logs") + "/",
        }
    )
    write_json(inputs / "task.json", {"parallel_workers": 1, "tasks": [task]})
    write_json(
        inputs / "task_manager_metadata.json",
        {
            "files": {
                "tasks": {
                    "title": "Task manager data",
                    "description": f"Pairwise scenario {sid}",
                    "path": rel(inputs / "task.json"),
                    "type": "json",
                    "properties": "tasks_properties",
                }
            }
        },
    )
    write_json(sdir / "scenario.json", {**s, "max_days": max_days})


def generate(scenarios: list[dict[str, Any]], max_days: int | None) -> None:
    for s in scenarios:
        generate_scenario(s, max_days)
    print(f"Entradas geradas para {len(scenarios)} cenário(s) em {rel(WORK_DIR)}/")


# --------------------------------------------------------------------------------------------------
# Execução
# --------------------------------------------------------------------------------------------------
def status_path(sid: str) -> Path:
    return scenario_dir(sid) / "status.json"


def read_status(sid: str) -> dict[str, Any] | None:
    p = status_path(sid)
    return read_json(p) if p.exists() else None


def find_milk_output(sid: str) -> Path | None:
    jsons = sorted((scenario_dir(sid) / "output/JSONs").glob("*.json"), key=lambda p: p.stat().st_mtime)
    return jsons[-1] if jsons else None


def collect_error_logs(sid: str, limit: int = 4000) -> str:
    """Junta trechos dos arquivos de erro do RuFaS para o diagnóstico."""
    chunks = []
    for p in sorted((scenario_dir(sid) / "logs").rglob("*")):
        if p.is_file() and "error" in p.name.lower():
            try:
                chunks.append(f"--- {p.name}\n{p.read_text(encoding='utf-8', errors='replace')[-limit:]}")
            except OSError:
                pass
    return "\n".join(chunks)[-limit:]


def run_scenario(sid: str, python: str, timeout: int | None) -> dict[str, Any]:
    sdir = scenario_dir(sid)
    for sub in ("output", "logs"):
        shutil.rmtree(sdir / sub, ignore_errors=True)
    (sdir / "logs").mkdir(parents=True, exist_ok=True)
    cmd = [
        python,
        "main.py",
        "-p", rel(sdir / "inputs/task_manager_metadata.json"),
        "-o", rel(sdir / "output") + "/",
        "-l", rel(sdir / "logs") + "/",
        "-g",
        "-i",
    ]
    started = time.time()
    status: dict[str, Any] = {"scenario_id": sid, "command": " ".join(cmd), "started_at": dt.datetime.now().isoformat()}
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    try:
        with open(sdir / "logs/stdout.txt", "w", encoding="utf-8") as out, open(
            sdir / "logs/stderr.txt", "w", encoding="utf-8"
        ) as err:
            proc = subprocess.run(cmd, cwd=REPO_ROOT, stdout=out, stderr=err, timeout=timeout, env=env)
        status["return_code"] = proc.returncode
        milk = find_milk_output(sid)
        if proc.returncode != 0:
            status["state"] = "failed"
            status["reason"] = f"RuFaS terminou com código {proc.returncode}"
        elif milk is None:
            status["state"] = "failed"
            status["reason"] = "execução terminou sem gerar o JSON de saída filtrado"
        else:
            status["state"] = "completed"
            status["output_file"] = rel(milk)
    except subprocess.TimeoutExpired:
        status["state"] = "failed"
        status["reason"] = f"tempo limite de {timeout}s excedido"
    except Exception:  # noqa: BLE001
        status["state"] = "failed"
        status["reason"] = traceback.format_exc()
    status["elapsed_s"] = round(time.time() - started, 1)
    if status["state"] == "failed":
        stderr_txt = (sdir / "logs/stderr.txt").read_text(encoding="utf-8", errors="replace")
        status["stderr_tail"] = stderr_txt[-4000:]
        status["error_logs"] = collect_error_logs(sid)
    write_json(status_path(sid), status)
    return status


def run(scenarios: list[dict[str, Any]], workers: int, force: bool, python: str, timeout: int | None) -> None:
    pending = []
    for s in scenarios:
        sid = s["scenario_id"]
        if not (scenario_dir(sid) / "inputs/task_manager_metadata.json").exists():
            raise SystemExit(f"{sid}: entradas não geradas. Rode o subcomando 'generate' antes.")
        st = read_status(sid)
        if st and st.get("state") == "completed" and not force:
            continue
        pending.append(sid)
    skipped = len(scenarios) - len(pending)
    print(f"Executando {len(pending)} cenário(s) com {workers} worker(s) ({skipped} já concluído(s) pulado(s))")
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run_scenario, sid, python, timeout): sid for sid in pending}
        for fut in as_completed(futures):
            st = fut.result()
            done += 1
            msg = "OK" if st["state"] == "completed" else f"FALHOU ({st.get('reason', '').splitlines()[0]})"
            print(f"[{done}/{len(pending)}] {st['scenario_id']}: {msg} em {st['elapsed_s']}s "
                  f"(total {round(time.time() - t0)}s)", flush=True)
    summarize(scenarios)


def summarize(scenarios: list[dict[str, Any]]) -> None:
    states: dict[str, list[str]] = {}
    for s in scenarios:
        st = read_status(s["scenario_id"])
        states.setdefault(st["state"] if st else "not_run", []).append(s["scenario_id"])
    for state, ids in sorted(states.items()):
        print(f"  {state}: {len(ids)}" + (f" -> {', '.join(ids)}" if state != "completed" else ""))
    if "failed" in states:
        print(f"  Diagnóstico de cada falha em {rel(WORK_DIR)}/<ID>/status.json e logs/")


# --------------------------------------------------------------------------------------------------
# Consolidação
# --------------------------------------------------------------------------------------------------
def extract_milk_records(output_json: Path) -> list[dict[str, Any]]:
    data = read_json(output_json)
    key = next((k for k in data if k.endswith(MILK_VARIABLE_SUFFIX)), None)
    if key is None:
        raise ValueError(f"variável '{MILK_VARIABLE_SUFFIX}' não encontrada em {output_json.name}")
    values = data[key]["values"] if isinstance(data[key], dict) else data[key]
    return values


def fmt(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        return round(value, 6)
    return value


def consolidate(scenarios: list[dict[str, Any]]) -> None:
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    protocol_rows = []
    failed_rows = []
    dataset_path = DATASET_DIR / "milk_dataset.csv"
    n_records = 0
    with open(dataset_path, "w", newline="", encoding="utf-8") as fds:
        writer = csv.DictWriter(fds, fieldnames=DATASET_COLUMNS)
        writer.writeheader()
        for s in scenarios:
            sid = s["scenario_id"]
            st = read_status(sid)
            if not st or st.get("state") != "completed":
                failed_rows.append(
                    {"SCENARIO_ID": sid, "STATE": st["state"] if st else "not_run",
                     "REASON": (st or {}).get("reason", "")}
                )
                continue
            try:
                records = extract_milk_records(REPO_ROOT / st["output_file"])
            except Exception as e:  # noqa: BLE001
                failed_rows.append({"SCENARIO_ID": sid, "STATE": "failed", "REASON": f"consolidação: {e}"})
                continue
            start = dt.date(s["start_year"], 1, 1)
            times_milked = s["milkings_per_day"]
            for r in records:
                # simulation_day é 0 no primeiro dia simulado (1º de janeiro do ano inicial)
                date = start + dt.timedelta(days=int(r["simulation_day"]))
                daily_milk = r.get("estimated_daily_milk_produced") or 0.0
                is_milking = bool(r.get("is_milking"))
                dmi = r.get("dry_matter_intake")
                writer.writerow(
                    {
                        "SCENARIO_ID": sid,
                        "COW_ID": r["cow_id"],
                        "PEN_ID": r["pen_id"],
                        "IS_MILKING": int(is_milking),
                        "PARITY": r["parity"],
                        "DAYS_IN_MILK": r["days_in_milk"],
                        # produção por ordenha (kg) = produção diária / ordenhas por dia
                        "MILK_PRODUCED": fmt(daily_milk / times_milked),
                        "MILK_LACTOSE": fmt(r.get("milk_lactose")),
                        "MILK_PROTEIN": fmt(r.get("milk_protein")),
                        "MILK_FAT": fmt(r.get("milk_fat")),
                        # kg de leite por kg de matéria seca ingerida
                        "FEED_EFFICIENCY": fmt(daily_milk / dmi) if dmi else "",
                        "MILKING_DATE": date.isoformat() if is_milking else "",
                        "DAILY_METHANE_EMISSION": fmt(r.get("enteric_methane")),  # g/dia
                        "DAILY_MILK_PRODUCED": fmt(daily_milk),  # kg/dia
                        "DATE": date.isoformat(),
                    }
                )
                n_records += 1
            protocol_rows.append(
                {
                    "SCENARIO_ID": sid,
                    "INITIAL_COW_NUM": s["cow_num"],
                    "SIMULATION_PERIOD": f"{s['start_year']}-01-01/{s['end_year']}-12-31",
                    "REGION": f"{FIPS_REGIONS[s['fips']]} (FIPS {s['fips']:05d})",
                    "COW_TIMES_MILKED_PER_DAY": s["milkings_per_day"],
                    "INITIAL_PARITY_DISTRIBUTION": "{} ({:.3f}/{:.3f}/{:.3f})".format(
                        s["parity_profile"], *PARITY_PROFILES[s["parity_profile"]]
                    ),
                    "INITIAL_MILKING_COW_FRACTION": s["milking_cow_pct"] / 100,
                    "FEEDING_SCENARIO_PROD_CUT": s["prod_cut_kg"],
                }
            )

    with open(DATASET_DIR / "protocol.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PROTOCOL_COLUMNS)
        w.writeheader()
        w.writerows(protocol_rows)
    with open(DATASET_DIR / "failed_scenarios.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["SCENARIO_ID", "STATE", "REASON"])
        w.writeheader()
        w.writerows(failed_rows)
    print(f"Consolidado: {len(protocol_rows)} cenário(s), {n_records} registros diários")
    print(f"  {rel(DATASET_DIR / 'protocol.csv')}")
    print(f"  {rel(dataset_path)}")
    if failed_rows:
        print(f"  {len(failed_rows)} cenário(s) fora do dataset -> {rel(DATASET_DIR / 'failed_scenarios.csv')}")


# --------------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------------
def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass
    parser =argparse.ArgumentParser(description="Simulações pairwise do RuFaS (154 cenários)")
    parser.add_argument("command", choices=["verify", "generate", "run", "consolidate", "all"])
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                        help="simulações em paralelo (padrão: metade dos núcleos)")
    parser.add_argument("--only", help="lista de IDs separados por vírgula, ex.: PW001,PW002")
    parser.add_argument("--force", action="store_true", help="reexecuta cenários já concluídos")
    parser.add_argument("--timeout", type=int, default=None, help="tempo limite por cenário, em segundos")
    parser.add_argument("--max-days", type=int, default=None,
                        help="TESTE: encurta cada simulação para N dias (não usar no dataset final)")
    parser.add_argument("--python", default=sys.executable, help="interpretador usado para rodar o RuFaS")
    args = parser.parse_args()

    scenarios = load_scenarios()
    if args.only:
        wanted = {x.strip().upper() for x in args.only.split(",") if x.strip()}
        unknown = wanted - {s["scenario_id"] for s in scenarios}
        if unknown:
            raise SystemExit(f"IDs desconhecidos: {', '.join(sorted(unknown))}")
        selected = [s for s in scenarios if s["scenario_id"] in wanted]
    else:
        selected = scenarios

    if args.command in ("verify", "all"):
        if not verify(scenarios) and args.command == "all":
            raise SystemExit("Matriz inválida; abortando.")
    if args.command in ("generate", "all"):
        generate(selected, args.max_days)
    if args.command in ("run", "all"):
        run(selected, args.workers, args.force, args.python, args.timeout)
    if args.command in ("consolidate", "all"):
        consolidate(selected)


if __name__ == "__main__":
    main()
