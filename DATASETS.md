# Dataset Descriptions

`jpinfectpy` provides access to three primary datasets derived from the [National Institute of Infectious Diseases (NIID)](https://www.niid.go.jp/niid/en/) Weekly Reports. This document details the contents, schema, and nature of each dataset.

## 1. Sex-Prefecture Data (`jp.load("sex")`)

This dataset tracks reported cases of infectious diseases disaggregated by **sex** and **prefecture**.

*   **Source**: Sentinel surveillance data (Files typically named `Syu_01_1.xls`).
*   **Time Range**: 1999 – 2023 (Bundled).
*   **Granularity**: Weekly.
*   **Schema**:

| Column | Type | Description |
| :--- | :--- | :--- |
| `prefecture` | `str` | Name of the prefecture (e.g., "Hokkaido", "Tokyo"). |
| `year` | `int` | Reporting year. |
| `week` | `int` | Reporting week (ISO-like). |
| `date` | `date` | Calculated Monday of the reporting week. |
| `disease` | `str` | Name of the disease (in English, e.g., "Influenza"). |
| `category` | `str` | Category of the count: `"total"`, `"male"`, `"female"`. |
| `count` | `int` | Number of reported cases. |

*   **Notes**:
    *   Categories are normalized to lowercase.
    *   Historical data (pre-2006) often contains "Unknown" sex categories which are cleaned or standardized where possible.

## 2. Place-Prefecture Data (`jp.load("place")`)

This dataset tracks reported cases disaggregated by **reporting sentinel type/location** (e.g., Hospital, Clinic) and **prefecture**.

*   **Source**: Sentinel surveillance data (Files typically named `Syu_02_1.xls`).
*   **Time Range**: 2001 – 2023 (Bundled).
*   **Granularity**: Weekly.
*   **Schema**:

| Column | Type | Description |
| :--- | :--- | :--- |
| `prefecture` | `str` | Name of the prefecture. |
| `year` | `int` | Reporting year. |
| `week` | `int` | Reporting week. |
| `date` | `date` | Calculated Monday of the reporting week. |
| `disease` | `str` | Name of the disease. |
| `category` | `str` | Sentinel type: `"total"`, `"hospital"`, `"clinic"`, etc. |
| `count` | `int` | Number of reported cases. |

## 3. Bullet Data (`jp.download("bullet", ...)` or `jp.read(...)`)

"Bullet" refers to the raw weekly bulletin CSVs. These files are typically text-heavy and may contain varied formatting depending on the week. They are not bundled due to their size and variability but can be downloaded and parsed.

*   **Source**: Raw CSV Weekly Reports.
*   **Time Range**: Available for download for recent years (e.g., 2024-2025).
*   **Granularity**: Weekly.
*   **Schema** (when parsed via `jp.read()`):
    *   Similar long-format structure to `sex` and `place` data where possible.
    *   Includes `year`, `week`, `date` columns inferred from the filename.
    *   Disease columns are melted into `disease`/`count` pairs.

## Data Processing Steps

All datasets undergo the following processing when loaded:
1.  **Header Resolution**: Legacy Excel headers (merged cells) are parsed to identify Disease and Category.
4. Reshaping: Data is "melted" from wide (pivot) format to long format for easier analysis.

## Combined Data (`jp.load_all()`)

This function fuses the historical `sex` dataset (1999-2023) with the most recent `bullet` data (2024-present).

*   **Logic**:
    *   Loads `sex` data and filters to `category="total"` (to match recent data).
    *   Downloads generic 2024+ weekly reports (if available).
    *   Combines them into a single timeline.
    *   Adds a `source` column: `"historical_sex"` or `"recent_bullet"`.
*   **Limitation**: Age-stratified data is **not available** in these standard datasets.

## Age Stratification

Users often request age-stratified data (e.g., cases by age group). Please note:
*   **Not Available**: The standard `Syu_01` (Sex) and "Bullet" (CSV) files provided by NIID do **not** contain age-group columns. They are stratified only by Prefecture and Sex (in older files) or just Prefecture (in recent rapid reports).
