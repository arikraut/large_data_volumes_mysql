# TDT4225 Project 3 — Geolife GPS Trajectory dataset with MongoDB

## Overview

This project is a course assignment implemented in **MongoDB + Python**. The work is organized around three deliverables:

1. **Data preparation & ingestion**: clean raw trajectory files and load them into MongoDB collections.
2. **Analytics queries**: answer a set of questions about the dataset using MongoDB queries and aggregation pipelines.
3. **Discussion/reporting**: document assumptions, results, and tradeoffs.

The code in this folder implements an end-to-end pipeline: dataset cleaning → collection creation with schema validation → bulk insertion of users/activities/trackpoints → query execution and export of results.

## Dataset

The implementation targets the **GeoLife GPS trajectory** format (`.plt` files with trackpoints) and optional `labels.txt` files for transportation-mode annotations (available only for a subset of users).

Expected directory structure (mirrors the dataset layout used in the course):

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
-   `databaseManager.py` — Connects to MongoDB, applies schema validation, creates indexes, inserts data, and implements queries.
-   `schemas.py` — MongoDB `$jsonSchema` validation rules for `User`, `Activity`, and `TrackPoint`.
-   `dataHelper.py` — Parsers/utilities for extracting activities and trackpoints, and for matching labeled intervals.
-   `prepareData.py` — Dataset cleaning utilities (trackpoint limits, altitude correction, duplicate handling).
-   `queryExecutor.py` — Runs the full query suite and formats output.
-   `DbConnector.py` — MongoDB connection helper.
-   `extractionTools.py` — Small utilities used during reporting/exploration (e.g., find largest files).

## Tech stack

-   **Python 3**
-   **MongoDB**

## Data model (collections)

Collections are created/validated by `DatabaseManager` using `$jsonSchema` rules in `schemas.py`.

### `User` (collection)

Document shape:

-   `_id` (string user id)
-   `has_labels` (boolean)

### `Activity` (collection)

Document shape:

-   `user_id` (string)
-   `start_date_time` (date)
-   `end_date_time` (date)
-   `transportation_mode` (string, optional; present only when labeled)

### `TrackPoint` (collection)

Document shape:

-   `activity_id` (ObjectId)
-   `user_id` (string) — denormalized for simpler user-centric queries
-   `date_time` (date)
-   `altitude` (int)
-   `location` (GeoJSON Point)
    -   `type: "Point"`
    -   `coordinates: [lon, lat]`

## Indexing and constraints

`DatabaseManager` sets up indexes to support data integrity and query performance:

-   `Activity`: unique compound index on `(user_id, start_date_time)` to prevent duplicate activities.
-   `TrackPoint`: unique compound index on `(activity_id, date_time)` to prevent duplicate trackpoints.
-   `TrackPoint`: index on `user_id` to accelerate user-level filtering.
-   `TrackPoint`: `2dsphere` index on `location` to support geo queries (e.g., Forbidden City visit detection).

## Data preparation decisions

The pipeline applies several cleaning/normalization rules before ingestion:

-   **Max trackpoints per activity**: files with more than `max_trackpoints` (default `2501`) are marked for deletion/skipping to satisfy assignment constraints.
-   **Invalid altitude normalization**: altitudes below `min_altitude` (default `-505`) are rewritten to an invalid sentinel (`-777`) before insertion.
-   **Duplicate timestamps inside a file**: `prepareData.py` tracks per-file timestamps to detect and skip duplicates.
-   **Transportation-mode labeling**: for users with `labels.txt`, a mode label is assigned when an activity’s `(start_time, end_time)` matches a labeled interval exactly. Otherwise, the activity is stored without a `transportation_mode` field.

## Query suite (what is implemented)

The analytics part is implemented as methods on `DatabaseManager` and executed from `queryExecutor.py`, including:

1. Counts of entities (`User`, `Activity`, `TrackPoint`)
2. Average number of activities per user
3. Top 20 users by activity count
4. Users who have used a taxi (`transportation_mode == "taxi"`)
5. Transportation modes and their counts
6. Year with the most activities and year with the most recorded hours
7. Total distance walked by a specific user in 2008 (Haversine, computed from trackpoint sequence)
8. Top 20 users by total altitude gained
9. Users with invalid activities (≥ 5-minute gaps between consecutive trackpoints)
10. Users who visited the Forbidden City (geo query using a bounding box)
11. Each user’s most-used transportation mode

## Data availability and reproducibility

The original coursework execution used a university-hosted environment and a large dataset. This repository does not include the full dataset nor a setup. The intent of this repo is to make the **collection design, ingestion logic, schema validation, and query/aggregation implementations** reviewable.

## Future work

If I were to continue this project beyond the course scope, I would prioritize the following improvements to strengthen reproducibility, security, and performance:

-   **Configuration and secrets management:** Move MongoDB connection parameters out of source code and into environment variables (or a `.env` file) to avoid hardcoded credentials and simplify setup.
-   **Portable local demo:** Provide a small, representative sample dataset (or a synthetic-data generator that preserves the data shape) so reviewers can run ingestion and queries without the full dataset.
-   **Operational robustness:** Add resumable ingestion (checkpointing), clearer idempotency guarantees, and more explicit error reporting around bulk insert duplicates.
-   **Containerized workflow:** Add Docker Compose for MongoDB + the ingestion/query runner, including initialization and optional sample data loading.
