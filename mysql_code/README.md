# TDT4225 Project 2 — Geolife GPS Trajectory dataset with MySQL

## Overview

This project is a course assignment implemented in **MySQL + Python**. The work is organized around three deliverables:

1. **Data preparation & ingestion**: clean raw trajectory files and load them into a relational schema.
2. **Analytics queries**: answer a set of questions about the dataset using SQL (including window functions).
3. **Discussion/reporting**: document assumptions, results, and tradeoffs.

The code in this folder implements an end-to-end pipeline: dataset cleaning → table creation → insertion of users/activities/trackpoints → query execution and export of results.

## Dataset

The implementation targets the **Microsoft Research GeoLife GPS Trajectories** dataset (PLT trajectory files, optional `labels.txt` transportation-mode annotations).

```
Data/
  000/
    Trajectory/
      *.plt
    labels.txt          # only for labeled users
  001/
  ...
labeled_ids.txt         # user IDs that have labels
```

## Repository contents (key files)

-   `main.py` — Orchestrates ingestion and runs the query suite (writes `results.txt`).
-   `databaseManager.py` — Creates tables, inserts data, and implements the required SQL queries.
-   `dataHelper.py` — Parsers/utilities for extracting times, labels, and trackpoints.
-   `prepareData.py` — Dataset cleaning utilities (line-count filtering, altitude correction, user preparation).
-   `DbConnector.py` — MySQL connection helper.

## Tech stack

-   **Python 3**
-   **MySQL**

## Schema

Three tables are created by `DatabaseManager.create_tables()`:

### `User`

-   `id` (PK)
-   `has_labels` (BOOLEAN)

### `Activity`

-   `id` (PK, auto-increment)
-   `user_id` (FK → `User.id`)
-   `transportation_mode` (nullable/`'NULL'` string used for “unknown/unlabeled”)
-   `start_date_time`, `end_date_time`
-   Uniqueness constraint: `UNIQUE(user_id, start_date_time)`

### `TrackPoint`

-   `id` (PK, auto-increment)
-   `activity_id` (FK → `Activity.id`)
-   `lat`, `lon`, `altitude`, `date_time`
-   Uniqueness constraint: `UNIQUE(activity_id, date_time)`

## Data preparation decisions

The pipeline makes a few explicit cleaning/normalization choices:

-   **Max trackpoints per activity**: `prepareData.clean_data()` removes trajectory files exceeding a fixed line threshold (configured as `2507`, accounting for a 6-line header and an end line). This aligns with common course constraints where an activity may be limited to ~2,500 trackpoints.
-   **Invalid altitude normalization**: `prepareData.fix_negative_alt()` rewrites altitudes below `-505` to `-777` (invalid sentinel) before loading.
-   **Transportation-mode labeling**: for users with `labels.txt`, a mode label is assigned when an activity’s `(start_time, end_time)` matches a labeled interval exactly. Otherwise, the activity is inserted with an “unknown” mode.

## Query suite (what is implemented)

The analytics part is implemented as methods on `DatabaseManager` and executed from `main.py`, including:

-   Row counts in each table (`User`, `Activity`, `TrackPoint`)
-   Average number of activities per user
-   Top users by number of activities
-   Activities per transportation mode + list of transportation modes used
-   Year with the most activities and year with the most recorded hours
-   Total distance walked by a given user in a given year (Haversine)
-   Top users by total altitude gained (uses SQL window functions + unit conversion)
-   Users with invalid activities (≥ 5-minute gaps between consecutive trackpoints)
-   Users who visited the Forbidden City (bounding-box filter)
-   Each user’s most-used transportation mode (window function ranking)

## Data availability and reproducibility

The original coursework execution used a university-hosted environment and a large dataset. This repository does not include the full dataset nor a setup. The intent of this repo is to make the **collection design, ingestion logic, schema validation, and query/aggregation implementations** reviewable.

## Notes and potential improvements

If I were to continue this project beyond the course scope, I would prioritize the following improvements to strengthen reproducibility, security, and performance:

-   moving DB credentials to environment variables (or a `.env` file) rather than hardcoding,
-   adding `CREATE INDEX` statements for query performance (e.g., on `Activity.user_id`, `TrackPoint.activity_id`, `TrackPoint.date_time`),
-   providing a small **sample dataset** or a **synthetic generator** to make the project runnable for reviewers without the full dataset,
-   adding Docker Compose for MySQL + the ingestion container.
