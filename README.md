Perfect ✅ — here’s your **GitHub-ready README** version of the PySpark + dbt audit migration exercise (formatted in Markdown for a clean, professional repo).
You can copy this directly into a `README.md` file for your study project or interview prep repo.

---

````markdown
# 🚀 Advanced Data Engineering Exercise — PySpark → dbt Audit Migration

## 📘 Context

You receive multiple **event log files** and **transaction files** each day.

Your current **PySpark job** performs the following:

1. Reads the daily event files (`events_YYYYMMDD.csv`) and transaction files (`transactions_YYYYMMDD.csv`).
2. Joins `events` ↔ `transactions` using `transaction_id`.
3. For each `load_date` and `file_name`, computes:
   - `nb_total`: total number of records  
   - `nb_invalid`: number of invalid records  
   - `error_rate` = `nb_invalid / nb_total`
4. Computes a **7-day rolling average** of `error_rate` by `file_name`.
5. For each `load_date`, ranks files by `error_rate` and selects the **Top 3**.
6. Writes a daily audit report and appends one row into the **central audit table**.

### 💡 Business Rule (Invalid Records)

A record is **invalid** if:

- `status != 'OK'`  
- OR `amount IS NULL`  
- OR `amount < 0`

---

## 🧾 Example Dataset

### `events_2025-10-24.csv`

| event_id | transaction_id | file_name             | status | event_time          |
| -------- | -------------- | --------------------- | ------ | ------------------- |
| e1       | t1             | events_2025-10-24.csv | OK     | 2025-10-24 01:00:00 |
| e2       | t2             | events_2025-10-24.csv | NOK    | 2025-10-24 02:00:00 |
| e3       | t3             | events_2025-10-24.csv | OK     | 2025-10-24 03:00:00 |

### `transactions_2025-10-24.csv`

| transaction_id | amount | customer_id | load_time           |
| -------------- | ------ | ----------- | ------------------- |
| t1             | 100.0  | c1          | 2025-10-24 00:59:00 |
| t2             | null   | c2          | 2025-10-24 01:59:00 |
| t3             | -10.0  | c1          | 2025-10-24 02:59:00 |

> _(Create similar files for 7+ days and different `file_name`s.)_

---

## 🧠 Step-by-Step Tasks

### **Step A — PySpark: Audit Job + Optimizations**

1. Write a PySpark job that:
   - Reads `events` and `transactions` (from S3 or local folder)
   - Joins on `transaction_id` (left join)
   - Applies the invalid record rule
   - Aggregates per `load_date` and `file_name`:  
     `nb_total`, `nb_invalid`, `error_rate`
2. Add:
   - A **7-day rolling error_rate** per `file_name` using `Window`
   - A **rank** column per `load_date`
3. Describe **two performance optimizations** (e.g. partitioning, broadcast join, persisting, schema definition).

> **Hint:**  
> For rolling window:  
> `Window.partitionBy("file_name").orderBy("load_date").rowsBetween(-6, 0)`

---

### **Step B — dbt Design: Data Modeling**

1. Draw your dbt folder structure (e.g., `staging/`, `marts/analytics/`, `schema.yml`).
2. Describe each model:
   - `stg_events`
   - `stg_transactions`
   - `mart_audit_daily`
   - `mart_audit_rolling`
   - `mart_audit_top3`
3. List columns for each model (name + type + grain).

> **Hint:**  
> - `mart_audit_daily`: 1 row per (`load_date`, `file_name`)  
> - `mart_audit_rolling`: 1 row per (`load_date`, `file_name`)  
> - `mart_audit_top3`: 1 row per (`load_date`, `rank ≤ 3`)

---

### **Step C — SQL Translation (Pseudo-SQL Only)**

Describe logic for these three models:

1. **`stg_events`** — cleaning, casting columns (which and why).  
2. **`mart_audit_daily`** — aggregation + error rate computation.  
3. **`mart_audit_rolling`** — rolling 7-day average using SQL window or cumulative logic.

> **Hint:**  
> ```sql
> AVG(error_rate) OVER (
>   PARTITION BY file_name
>   ORDER BY load_date
>   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
> )
> ```

---

### **Step D — Top 3 by Day**

1. Build `mart_audit_top3` that returns, for each `load_date`, the 3 files with the highest `error_rate`.  
2. Choose between `TABLE` or `VIEW` materialization and justify your choice.

> **Hint:**  
> Use:
> ```sql
> ROW_NUMBER() OVER (PARTITION BY load_date ORDER BY error_rate DESC)
> ```
> and filter on `rank <= 3`.

---

### **Step E — dbt Tests & Monitoring**

1. Propose **4+ dbt tests** in `schema.yml` for:
   - `mart_audit_daily`
   - `mart_audit_rolling`
2. Configure **source freshness** for `events` and `transactions`.
3. Define **2 alert rules** (condition + notification channel).

> **Hint:** Example tests:
> - `not_null` on `load_date`
> - `greater_than` on `nb_total > 0`
> - `accepted_range` on `error_rate`
> - `unique` on (`file_name`, `load_date`)

---

### **Step F — CI/CD & Industrialization**

1. Describe your deployment pipeline:
   - `dbt run`, `dbt test`, `dbt docs generate`
   - Orchestration (Airflow / dbt Cloud)
   - Pre- and post-checks
2. Explain handling of **incremental models** for daily file ingestion  
   (unique key = `file_name` + `load_date`).

---

## 🧮 Evaluation Criteria

The interviewer will assess:

- ✅ Correct business logic (invalid rules, error rate)
- ✅ PySpark ↔ SQL translation (window, rolling, rank)
- ✅ dbt modeling quality (staging/marts, grain consistency)
- ✅ Performance best practices (partitioning, broadcast, cache)
- ✅ Data quality coverage (tests, freshness)
- ✅ CI/CD and monitoring setup

---

## 📦 Expected Deliverables

1. Pseudo-code or PySpark script for **Step A**  
2. dbt structure + pseudo-SQL logic for **Steps B & C**  
3. `schema.yml` with tests for **Step E**  
4. Short paragraph on **optimizations & CI/CD**

---

## 🧩 Optional Hints

- Avoid division by zero: `NULLIF(nb_total, 0)` or `CASE WHEN nb_total = 0 THEN NULL ... END`
- If warehouse doesn’t support `ROWS BETWEEN`, pre-compute `day_index` or self-join on date window
- For top 3: use `ROW_NUMBER() OVER (PARTITION BY load_date ORDER BY error_rate DESC)`

---

## 🧰 Folder Structure Example

````

.
├── data/
│   ├── events_2025-10-24.csv
│   ├── transactions_2025-10-24.csv
│   └── ...
├── pyspark_job/
│   └── audit_job.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_events.sql
│   │   │   ├── stg_transactions.sql
│   │   ├── marts/
│   │   │   ├── mart_audit_daily.sql
│   │   │   ├── mart_audit_rolling.sql
│   │   │   ├── mart_audit_top3.sql
│   ├── schema.yml
│   └── dbt_project.yml
└── README.md

```

---

## 🧭 How to Use

- 🧑‍💻 Try each step before peeking at any solution.  
- 🔁 Once you finish Step A, compare your PySpark code with an optimized version.  
- 🧱 Then design the dbt models (Steps B–F) and simulate deployment.  
- 🧠 Goal: prove end-to-end understanding of PySpark + dbt data pipelines.

---

**Good luck — and remember: optimize first, automate second!** 🚀
```

---
