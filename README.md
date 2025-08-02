# 🏗️ ETL Data Warehouse Project

## 📚 Overview  
This repository implements a **three-tier ETL architecture** using PostgreSQL, based on the **Bronze → Silver → Gold** model:

- 🥉 **Bronze**: Raw CSV ingestion with minimal validation  
- 🥈 **Silver**: Cleaned, typed, and de-duplicated staging tables  
- 🥇 **Gold**: Final analytical views (dimensions, facts)

---

## 📁 Repository Structure

```text
etl-warehouse-project/
├── 01_datasets/           # Source CSVs (CRM & ERP)
├── 02_bronze/             # Bronze schema & load scripts
│   ├── schema.sql
│   └── loadbronze.sql
├── 03_silver/             # Silver schema & transformation scripts
│   ├── schema.sql
│   └── transform_silver.sql
├── 04_gold/               # Gold views & verification scripts
│   ├── views_gold.sql
│   └── verify_gold.sql
├── 05_utilities/          # Common error-checking and helper scripts
│   └── check_errors.sql
└── docs/                  # Project documentation
    └── README.md
---

## 🚀 Features

- Organized Bronze → Silver → Gold architecture
- Modular SQL scripts for easy debugging and reusability
- Source-agnostic design: supports both CRM and ERP CSVs
- Reproducible pipeline using simple `psql` commands
- Clean separation of concerns across ETL layers

---
