# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/renato.santojpg@gmail.com/databrick-test/requirements.txt",
# ]
# ///
# MAGIC %md
# MAGIC # ViaCEP ETL Pipeline
# MAGIC
# MAGIC This notebook demonstrates an ETL pipeline that queries the **ViaCEP** public API
# MAGIC for 5 Brazilian postal codes (CEPs).
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC  ┌─────────────────────┐
# MAGIC  │   ViaCepAdapter      │  ← HTTP adapter (one request per CEP)
# MAGIC  └────────┬────────────┘
# MAGIC           │  raw JSON
# MAGIC           ▼
# MAGIC  ┌─────────────────────┐
# MAGIC  │   Extract layer      │  ← picks relevant fields from the raw object
# MAGIC  └────────┬────────────┘
# MAGIC           │  dict[str, str]
# MAGIC           ▼
# MAGIC  ┌─────────────────────┐
# MAGIC  │   Transform layer    │  ← formats fields into a unique address string
# MAGIC  └────────┬────────────┘
# MAGIC           │  str
# MAGIC           ▼
# MAGIC  ┌─────────────────────┐
# MAGIC  │   Load layer         │  ← sends to AWS SQS (or stdout fallback)
# MAGIC  └─────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — install dependencies and add `src` to the path

# COMMAND ----------

import sys
import os

# When running in a Databricks cluster the repository is checked out under
# /Workspace/Repos/<user>/<repo>.  We add the repo root so that `src` is
# importable.  When running locally the CWD is usually the repo root already.
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) \
    if "__file__" in dir() else os.getcwd()

if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Define the 5 CEPs to process

# COMMAND ----------

CEPS = [
    "01310100",  # Av. Paulista, São Paulo / SP
    "20040020",  # Centro, Rio de Janeiro / RJ
    "30112000",  # Centro, Belo Horizonte / MG
    "40020010",  # Centro, Salvador / BA
    "80020010",  # Centro, Curitiba / PR
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Import pipeline components

# COMMAND ----------

from src.adapters.viacep_adapter import ViaCepAdapter
from src.etl.extract import extract
from src.etl.transform import transform
from src.etl.load import load

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Run the ETL pipeline for each CEP

# COMMAND ----------

adapter = ViaCepAdapter()
results = []

for cep in CEPS:
    print(f"\n{'='*60}")
    print(f"Processing CEP: {cep}")

    # --- Extract ---------------------------------------------------
    raw = adapter.get(cep)
    print(f"  [Extract] raw fields: {list(raw.keys())}")
    extracted = extract(raw)
    print(f"  [Extract] extracted : {extracted}")

    # --- Transform -------------------------------------------------
    address_string = transform(extracted)
    print(f"  [Transform] result  : {address_string}")

    # --- Load ------------------------------------------------------
    # Set the environment variable SQS_QUEUE_URL (or pass queue_url=)
    # to route messages to a real AWS SQS queue.
    load_result = load(address_string)
    print(f"  [Load] status       : {load_result['status']}")

    results.append({
        "cep": cep,
        "address": address_string,
        "load_status": load_result["status"],
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Summary

# COMMAND ----------

print("\n" + "="*60)
print("Pipeline summary")
print("="*60)
for r in results:
    print(f"  {r['cep']}  →  {r['address']}  [{r['load_status']}]")
