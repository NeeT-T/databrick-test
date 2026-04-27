# databrick-test

A Databricks workspace that runs an **ETL pipeline** against the [ViaCEP](https://viacep.com.br) public API, processing 5 Brazilian postal codes (CEPs).

## Architecture

```
ViaCepAdapter  ──►  Extract  ──►  Transform  ──►  Load
   (HTTP)           (fields)      (string)        (SQS / stdout)
```

| Layer | Location | Responsibility |
|-------|----------|----------------|
| **Adapter** | `src/adapters/viacep_adapter.py` | HTTP client that fetches a CEP from the ViaCEP REST API |
| **Extract** | `src/etl/extract.py` | Selects the relevant fields from the raw API response object |
| **Transform** | `src/etl/transform.py` | Formats the extracted fields into a single human-readable address string |
| **Load** | `src/etl/load.py` | Publishes the string to an AWS SQS queue; falls back to stdout when SQS is not configured |

## Project layout

```
databrick-test/
├── notebooks/
│   └── viacep_etl.py       # Main Databricks notebook
├── src/
│   ├── adapters/
│   │   └── viacep_adapter.py
│   └── etl/
│       ├── extract.py
│       ├── transform.py
│       └── load.py
├── tests/
│   └── test_etl.py
└── requirements.txt
```

## Running in Databricks

1. Import this repository into a Databricks Repo (`Repos → Add Repo`).
2. Open `notebooks/viacep_etl.py` in the Databricks UI.
3. Attach it to a cluster (Python 3.8+).
4. *(Optional)* Set the `SQS_QUEUE_URL` environment variable on the cluster to route messages to AWS SQS.
5. Click **Run All**.

## Running locally

```bash
pip install -r requirements.txt
python notebooks/viacep_etl.py
```

## Running the tests

```bash
pip install pytest
python -m pytest tests/ -v
```

## CEPs used

| CEP | City | State |
|-----|------|-------|
| 01310-100 | São Paulo | SP |
| 20040-020 | Rio de Janeiro | RJ |
| 30112-000 | Belo Horizonte | MG |
| 40020-010 | Salvador | BA |
| 80020-010 | Curitiba | PR |

## Load layer — SQS configuration

Set the `SQS_QUEUE_URL` environment variable (or pass `queue_url=` directly to `load()`) before running the pipeline.  When the variable is absent or empty the pipeline prints the address strings to stdout, which is the default behaviour in Databricks Community Edition.
