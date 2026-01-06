# TDT4225 Projects — Geolife GPS Trajectory dataset with MongoDB and MySQL

## Overview

This repository contains two database course projects that implement the same workflow using two different data storage paradigms:

-   **Relational** implementation in **MySQL**
-   **Document/NoSQL** implementation in **MongoDB**

Both projects are built around the **Microsoft Research GeoLife GPS Trajectories** dataset (raw `.plt` trajectory files, with `labels.txt` transportation-mode annotations available for a subset of users).

Both projects follow the course structure:

1. **Data cleaning + ingestion** into a defined schema/data model
2. **Querying / analytics** to answer a fixed set of questions about the dataset

---

## Repository structure

-   `mysql_code/` — MySQL schema + ingestion + SQL query suite
    -   See `mysql_code/README.md` for schema, run toggles, and query overview
-   `mongodb_code/` — MongoDB schema validation + ingestion + aggregation/query suite
    -   See `mongodb_code/README.md` for collection design, indexes, and query overview

---

### MySQL

-   **Schema + constraints**: `mysql_code/databaseManager.py` (table definitions, uniqueness, FK structure)
-   **ETL decisions**: `mysql_code/prepareData.py` and `mysql_code/dataHelper.py`
-   **Query implementations**: `mysql_code/databaseManager.py`

### MongoDB

-   **Schema validation**: `mongodb_code/schemas.py` (`$jsonSchema` rules)
-   **Indexing strategy**: `mongodb_code/databaseManager.py`
-   **Aggregation/query implementations**: `mongodb_code/queryExecutor.py` + `mongodb_code/databaseManager.py`

---

## Tech stack

-   **Python 3**
-   **MySQL**
-   **MongoDB**

---

## Reproducibility and data availability

These projects were originally executed against a **large dataset hosted in a university-managed environment**. The dataset is **not included** in this public repository due to size, and I have **not packaged a fully automated local setup**.

That said, this repository still includes:

-   complete schema / model definitions (SQL + MongoDB validation)
-   the full ingestion logic and cleaning rules
-   the full query/aggregation implementations

---

## Design intent

The two subprojects intentionally solve the same style of tasks in different database systems so the tradeoffs are visible:

-   **Relational modeling + SQL analytics** (joins, constraints, window functions)
-   **Document modeling + aggregation pipelines** (schema validation, indexing strategy, geospatial querying)

---

## Future improvements

If I were to extend this repository beyond coursework, the highest-impact additions would be:

-   Provide a **small sample dataset** or **synthetic dataset generator** to enable “clone → run” execution
-   Add **Docker Compose** for MySQL + MongoDB and a one-command pipeline runner
-   Move database connection parameters to **environment variables** (`.env`) and document configuration
-   Add a lightweight **CI check** (linting + basic unit tests for parsers/helpers)

---
