# Firmable Australia ETL Pipeline

## Overview

This repository contains an end-to-end ETL pipeline for integrating and analyzing Australian company data from Common Crawl and ABR. The solution is designed for scalability, data quality, and extensibility, and demonstrates both traditional and AI-powered entity matching.

---

## Database Schema

The PostgreSQL schema is designed to support raw ingestion, entity matching, and analytics.  
**Key tables:**
- `raw_data.common_crawl_companies`
- `raw_data.abr_companies`
- `processed_data.companies`
- `processed_data.entity_matches`
- `analytics.data_quality_metrics`

**See [`ETL_PIPELINE_AUS/sql/schema_postgres.sql`](ETL_PIPELINE_AUS/sql/schema_postgres.sql) for full DDL.**

---

## Pipeline Architecture

```mermaid
flowchart TD
    subgraph Extraction
        A1[Extract ABR XML] --> B1[Clean & Deduplicate ABR Data]
        A2[Extract Common Crawl] --> B2[Clean & Deduplicate CC Data]
    end

    B1 --> C[Load ABR to Postgres]
    B2 --> D[Load CC to Postgres]
    C & D --> E[Integrated Raw Data in Postgres]

    E --> F1[NLP/Fuzzy Matching]
    F1 --> F2[LLM Matching (OpenAI GPT)]
    F2 --> G[Entity Matches Table]

    G --> H[dbt Data Quality Tests]
    H --> I[Analytics & Reporting]

    subgraph Monitoring & Orchestration
        J1[Airflow Scheduler]
        J2[dbt Docs & Tests]
        J3[Logs]
    end

    J1 -.-> A1
    J1 -.-> A2
    J1 -.-> C
    J1 -.-> D
    J1 -.-> F1
    J1 -.-> F2
    J1 -.-> H
    J2 -.-> H
    J3 -.-> I
```

**Description:**  
- Extraction scripts pull data from ABR and Common Crawl.
- Cleaning scripts standardize and deduplicate records.
- Cleaned data is loaded into PostgreSQL.
- NLP/fuzzy and LLM-based entity matching are performed in sequence.
- dbt models and tests ensure data quality.
- Analytics and reporting can be built on top of the processed data.
- Airflow, dbt, and logs provide orchestration and monitoring.

---

## Technology Justification

- **Airflow**: Chosen for its robust scheduling, orchestration, and monitoring capabilities, making it ideal for production-grade ETL pipelines.
- **Python**: Used for extraction, cleaning, and matching due to its rich ecosystem for data processing and ease of integration with APIs and databases.
- **dbt**: Enables modular, testable, and version-controlled SQL transformations and data quality checks, which are essential for reliable analytics.
- **PostgreSQL**: Selected for its reliability, advanced indexing, and support for complex queries and analytics.
- **NLP & Fuzzy Logic**: Employed for entity matching to handle variations in company names, addresses, and other fields, improving match rates through natural language processing and approximate string matching techniques.
- **LLM (OpenAI GPT-3.5/4)**: Demonstrated for advanced entity matching, leveraging natural language understanding to further improve match rates beyond traditional fuzzy and NLP-based methods.

---

## AI Model Used & Rationale

We use OpenAI's GPT-3.5/4 for entity matching demonstration.  
**Rationale:**  
- LLMs can understand context, handle variations in company names and addresses, and provide confidence scores and explanations for matches.
- This approach is especially valuable for smaller datasets or high-value matching tasks where accuracy is critical.

**See [`llm_entity_matching_demo.py`](../llm_entity_matching_demo.py) for a working example, including prompt, API call, and sample output.**

---

## Setup & Running Instructions

1. **Clone the repository**
   ```sh
   git clone https://github.com/yourusername/firmable-aus-etl.git
   cd firmable-aus-etl
   ```

2. **Set up Python environment**
   ```sh
   python -m venv venv
   source venv/bin/activate
   pip install -r ETL_PIPELINE_AUS/requirements.txt
   ```

3. **Set up PostgreSQL**
   - Create the database and user as per your environment.
   - Run the schema DDL:
     ```sh
     psql -U your_user -d your_db -f ETL_PIPELINE_AUS/sql/schema_postgres.sql
     ```

4. **Run the ETL pipeline**
   - Configure Airflow and start the scheduler.
   - Trigger the DAG in Airflow UI or via CLI.

5. **Run dbt data quality tests**
   ```sh
   cd firmable_dbt
   dbt test
   ```

6. **Run LLM entity matching demo**
   ```sh
   python ../llm_entity_matching_demo.py
   ```

---

## Entity Matching Approach

- **Traditional:** Uses rule-based, NLP, and fuzzy logic matching on company names, addresses, and ABNs to identify potential matches, leveraging techniques such as tokenization, similarity scoring, and approximate string matching.
- **LLM-based:** Uses OpenAI's GPT to compare company records and provide a match decision with confidence and reasoning, further enhancing accuracy for complex or ambiguous cases.

---

## Design Choices

- Modular pipeline for easy maintenance and extension.
- dbt for SQL transformations and data quality, ensuring analytics-ready data.
- LLM integration as a demonstration of how AI can enhance entity resolution.

---

## IDE Used

- Developed using **PyCharm** and **VSCode** for Python and SQL development.

---

## File Structure

- `ETL_PIPELINE_AUS/scripts/` — Extraction, cleaning, loading, and matching scripts
- `ETL_PIPELINE_AUS/sql/` — Database schema (DDL)
- `dags/` — Airflow DAGs
- `firmable_dbt/` — dbt models and tests
- `llm_entity_matching_demo.py` — LLM entity matching demonstration
- `README.md` — Project documentation

---

## Contact

For any questions, please open an issue or contact devyamehrotra123@gmail.com. 