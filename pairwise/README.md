# Dataset pairwise de produção de leite (154 cenários)

Fork rationale: this folder automates the 154 pairwise RuFaS simulations described in
*"Geração de Dataset de Produção de Leite no RuFaS"* (IFG – Câmpus Inhumas). The goal is to build a daily,
per-cow milk production dataset for training a Foundation Model. Forked from `RuminantFarmSystems/RuFaS` (`main`).

## Como rodar

Na raiz do repositório, com Python 3.12 ou 3.13:

```bash
python -m venv venv
venv\Scripts\activate            # Windows  (Linux/Mac: source venv/bin/activate)
pip install .
python pairwise/run_pairwise.py all --workers 4
```

| Subcomando    | O que faz |
|---------------|-----------|
| `verify`      | Confere a matriz: 154 cenários, níveis de cada fator e cobertura de 834/834 pares (Tabela 4). |
| `generate`    | Cria `pairwise/work/<ID>/inputs/` com os arquivos de entrada de cada cenário. |
| `run`         | Roda os cenários em paralelo (`--workers`). É retomável, porque pula os já concluídos (use `--force` para refazê-los). |
| `consolidate` | Gera `pairwise/dataset/protocol.csv`, `milk_dataset.csv` e `failed_scenarios.csv`. |
| `all`         | Faz tudo em sequência. |

Opções úteis:
- `--only PW001,PW002` limita a execução a alguns cenários.
- `--timeout 7200` define um tempo limite por cenário, em segundos.
- `--max-days 30` serve **só para teste**: encurta as simulações para 30 dias. Depois de um teste, rode `generate` de novo sem essa opção.

As falhas são diagnosticadas em `pairwise/work/<ID>/status.json` (motivo, final do stderr e logs de erro do RuFaS)
e na pasta `pairwise/work/<ID>/logs/`. Cenários com falha não entram no `milk_dataset.csv` nem no `protocol.csv`.

## Como cada fator vira entrada do RuFaS

O cenário base é o `example_freestall`. Tudo o que não aparece na tabela abaixo fica igual ao exemplo.

| Fator (PDF)              | Arquivo / campo do RuFaS |
|--------------------------|--------------------------|
| Vacas (10/50/100)        | `animal.herd_information.cow_num` e `herd_num`. Os limites de venda e compra seguem a proporção do exemplo (`n+1` e `int(0,95·n)`). |
| Período (5 anos)         | `config.start_date = AAAA:1` e `config.end_date = AAAA:<31/dez>` |
| FIPS                     | `config.FIPS_county_code` (o RuFaS deriva a região da curva de lactação a partir do código) |
| Ordenhas/dia             | `animal.animal_config.management_decisions.cow_times_milked_per_day` |
| Perfil de paridade       | `animal.herd_information.parity_fractions`. A fatia "3ª ou mais" é repartida entre as chaves 3, 4 e 5 na proporção do exemplo (0,18 : 0,11 : 0,09). |
| Lactação inicial         | `animal.herd_information.milking_cow_fraction` |
| Corte (0/0,25/0,50 kg)   | `feed.ration_formulation_parameters.milk_reduction_maximum` |

A semente aleatória é fixa (42) em todos os cenários.

## Colunas do `milk_dataset.csv`

Cada linha é uma vaca em um dia, a partir da variável `milk_data_at_milk_update` do RuFaS.

| Coluna | Origem |
|---|---|
| `COW_ID`, `PEN_ID`, `PARITY`, `DAYS_IN_MILK` | campos homônimos do RuFaS |
| `IS_MILKING` | 1 se `days_in_milk > 0` |
| `DAILY_MILK_PRODUCED` | `estimated_daily_milk_produced` (kg/dia) |
| `MILK_PRODUCED` | produção por ordenha = diária ÷ ordenhas/dia (kg) |
| `MILK_LACTOSE`, `MILK_PROTEIN`, `MILK_FAT` | kg/dia |
| `FEED_EFFICIENCY` | leite diário ÷ ingestão de matéria seca (kg/kg) |
| `DAILY_METHANE_EMISSION` | metano entérico da vaca (g/dia) |
| `DATE` | data do registro (`simulation_day` 0 corresponde a 1º de janeiro do ano inicial) |
| `MILKING_DATE` | igual a `DATE` quando a vaca está em lactação; vazio quando está seca |

### Alteração no código do RuFaS

O RuFaS original informa metano e ingestão de matéria seca apenas por baia. Para obter `DAILY_METHANE_EMISSION` e
`FEED_EFFICIENCY` por vaca, este fork acrescenta dois campos opcionais ao registro diário de leite de cada vaca:
`enteric_methane` e `dry_matter_intake`. A mudança está em `MilkProductionStatistics`, `Animal.milk_statistics` e
`AnimalModuleReporter.report_milk`. Ela não altera nenhum cálculo do modelo, só o que é reportado.

Observação: a ingestão de matéria seca vem da ração da baia. Com o modelo IPCC (o padrão do exemplo), por isso, o
metano é o mesmo para todas as vacas em lactação de uma mesma baia no mesmo dia.
