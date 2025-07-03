# Firmable Australia ETL Pipeline

A comprehensive data pipeline for extracting, transforming, and integrating Australian company data from Common Crawl and the Australian Business Register (ABR). This solution demonstrates advanced entity matching using NLP, fuzzy logic, and LLM technologies.

## 🎯 Project Goals

- **Data Integration**: Extract, clean, and integrate Australian company data from public sources
- **Entity Matching**: Implement entity matching using fuzzy logic, TF-IDF, and LLM methods
- **Data Quality**: Ensure data quality through dbt models and testing
- **Workflow Orchestration**: Use Apache Airflow for pipeline management
- **Security**: Implement environment-based configuration for secure credential management

## 🏗️ Architecture

```mermaid
flowchart TD
    subgraph "Data Sources"
        DS1[ABR XML Data]
        DS2[Common Crawl Data]
    end
    
    subgraph "Extraction Layer"
        E1[ABR Extractor]
        E2[Common Crawl Extractor]
    end
    
    subgraph "Processing Layer"
        P1[Data Cleaning]
        P2[Deduplication]
        P3[Normalization]
    end
    
    subgraph "Storage Layer"
        S1[PostgreSQL Raw Data]
        S2[Processed Data]
    end
    
    subgraph "Matching Engine"
        M1[Exact Matching]
        M2[Fuzzy/NLP Matching]
        M3[LLM Matching]
    end
    
    subgraph "Quality & Analytics"
        Q1[dbt Models]
        Q2[Data Quality Tests]
        Q3[Analytics Dashboard]
    end
    
    subgraph "Orchestration"
        O1[Airflow DAGs]
        O2[Monitoring]
        O3[Logging]
    end
    
    DS1 --> E1
    DS2 --> E2
    E1 --> P1
    E2 --> P1
    P1 --> P2
    P2 --> P3
    P3 --> S1
    S1 --> M1
    M1 --> M2
    M2 --> M3
    M3 --> S2
    S2 --> Q1
    Q1 --> Q2
    Q2 --> Q3
    O1 --> E1
    O1 --> E2
    O1 --> P1
    O1 --> M1
    O2 --> Q2
    O3 --> Q3
```

## 🗄️ Database Schema

### Raw Data Schema (`raw_data`)

```sql
-- Common Crawl companies
CREATE TABLE raw_data.common_crawl_companies (
    id SERIAL PRIMARY KEY,
    website_url VARCHAR(500) NOT NULL,
    company_name VARCHAR(255),
    industry VARCHAR(500),
    extracted_text TEXT,
    crawl_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ABR companies
CREATE TABLE raw_data.abr_companies (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(11) UNIQUE NOT NULL,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(500),
    entity_status VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    suburb VARCHAR(100),
    state VARCHAR(3),
    postcode VARCHAR(4),
    start_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Processed Data Schema (`processed_data`)

```sql
-- Unified companies
CREATE TABLE processed_data.companies (
    id SERIAL PRIMARY KEY,
    unified_name VARCHAR(255) NOT NULL,
    primary_abn VARCHAR(11),
    website_url VARCHAR(100000),
    industry VARCHAR(500),
    entity_type VARCHAR(100),
    entity_status VARCHAR(20),
    address JSONB,
    confidence_score DECIMAL(4,3),
    data_sources VARCHAR(50)[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Entity matches
CREATE TABLE processed_data.entity_matches (
    id SERIAL PRIMARY KEY,
    cc_company_id INTEGER REFERENCES raw_data.common_crawl_companies(id),
    abr_company_id INTEGER REFERENCES raw_data.abr_companies(id),
    unified_company_id INTEGER REFERENCES processed_data.companies(id),
    match_type VARCHAR(20),
    confidence_score DECIMAL(4,3),
    matching_criteria JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Analytics Schema (`analytics`)

```sql
-- Data quality metrics
CREATE TABLE analytics.data_quality_metrics (
    id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(50),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    threshold_value DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🤖 Entity Matching Strategy

### Multi-Stage Matching Approach

1. **Exact Match**: ABN or website URL exact matches
2. **Fuzzy Match**: Company names with >85% similarity using rapidfuzz
3. **NLP Match**: Natural language processing for semantic similarity
4. **LLM-Enhanced Match**: OpenAI GPT for complex business logic and reasoning

### LLM Integration

#### OpenAI GPT-4 Implementation

```python
COMPANY_MATCHING_PROMPT = """
You are an expert in Australian business entity matching. 
Given two company records, determine if they represent the same business entity.

Company A (Common Crawl):
- Name: {cc_name}
- Website: {cc_website}
- Industry: {cc_industry}

Company B (ABR):
- Name: {abr_name}
- ABN: {abr_abn}
- Entity Type: {abr_type}
- Address: {abr_address}

Consider:
1. Name variations (abbreviations, legal suffixes, trading names)
2. Geographic proximity
3. Industry alignment
4. Business entity types

Respond with JSON:
{
  "is_match": boolean,
  "confidence": float (0-1),
  "reasoning": "explanation",
  "unified_name": "suggested canonical name"
}
"""
```

### Performance Optimization

- **Batch Processing**: Group similar queries to optimize API costs
- **Parallel Processing**: Use ThreadPoolExecutor for concurrent matching
- **Blocking Strategy**: Block by first two letters of company names for efficiency
- **Caching**: Store LLM results to avoid repeated calls
- **Database Indexing**: Optimized indexes for fast lookups

## 📊 Data Quality & Monitoring

### Quality Metrics

- **Completeness**: Percentage of required fields populated
- **Accuracy**: Entity matching confidence scores
- **Consistency**: Data format compliance and standardization
- **Timeliness**: Pipeline execution time and data freshness

### dbt Data Quality Tests

```yaml
version: 2

models:
  - name: abr_companies_quality
    description: "ABR companies table with data quality tests"
    columns:
      - name: abn
        tests:
          - unique
          - not_null
      - name: entity_name
        tests:
          - not_null
      - name: postcode
        tests:
          - not_null
```

### Monitoring Dashboard

Access monitoring tools:
- **Airflow UI**: http://localhost:8080
- **dbt Docs**: http://localhost:8081
- **Database Metrics**: Custom PostgreSQL monitoring

## 🛠️ Technology Stack

### Core Technologies

- **Python 3.12**: Primary development language
- **PostgreSQL**: Robust relational database with advanced indexing
- **Apache Airflow**: Workflow orchestration and scheduling
- **dbt**: Data transformation and quality testing
- **SQLAlchemy**: Database ORM and connection management

### Data Processing

- **pandas**: Data manipulation and analysis
- **rapidfuzz**: Fast fuzzy string matching
- **OpenAI GPT-4**: Advanced entity matching and reasoning
- **psycopg2**: PostgreSQL adapter

### Development & Testing

- **pytest**: Unit and integration testing
- **pytest-mock**: Mocking for isolated testing
- **Git**: Version control and collaboration

## 📁 Project Structure

```
FirmableProjectsAustrlia/
├── ETL_PIPELINE_AUS/
│   ├── scripts/
│   │   ├── extraction/          # Data extraction modules
│   │   │   ├── extract_abr_xml.py
│   │   │   └── extract_common_crawl.py
│   │   ├── cleaning/            # Data cleaning and normalization
│   │   │   ├── clean_abr.py
│   │   │   └── clean_common_crawl.py
│   │   ├── loading/             # Database loading scripts
│   │   │   └── loadingcsv_topostgre_fast.py
│   │   ├── matching/            # Entity matching algorithms
│   │   │   ├── entity_matching.py
│   │   │   ├── entity_matching_tfidf.py
│   │   │   └── entity_matching_spark.py
│   │   ├── tests/               # Unit and integration tests
│   │   │   └── test_etl_pipeline.py
│   │   └── utils/               # Utility functions
│   ├── data/                    # Data files and directories
│   │   ├── abr/
│   │   │   ├── raw/
│   │   │   └── cleaned/
│   │   ├── common_crawl/
│   │   │   ├── raw/
│   │   │   └── cleaned/
│   │   └── entity_matching/
│   ├── sql/                     # Database schema and DDL
│   │   └── schema_postgres.sql
│   ├── logs/                    # Pipeline execution logs
│   └── requirements.txt         # Python dependencies
├── dags/                        # Airflow DAGs
│   └── etl_pipeline.py
├── firmable_dbt/                # dbt models and tests
│   ├── models/
│   └── dbt_project.yml
├── llm_entity_matching_demo.py  # LLM demonstration script
├── .gitignore                   # Git ignore rules
└── README.md                    # Project documentation
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 13+
- Apache Airflow 2.0+
- OpenAI API key (for LLM features)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/devyamehrotra/firmable-aus-etl.git
   cd firmable-aus-etl
   ```

2. **Set up Python environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r ETL_PIPELINE_AUS/requirements.txt
   ```

3. **Configure database**
   ```bash
   # Create database and user
   createdb newdb
   psql -U postgres -d newdb -f ETL_PIPELINE_AUS/sql/schema_postgres.sql
   ```

4. **Set environment variables**
   ```bash
   export OPENAI_API_KEY="your_openai_api_key"
   export DB_PASSWORD="your_postgres_password"
   ```

### Running the Pipeline

1. **Start Airflow**
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

2. **Trigger the ETL pipeline**
   - Open Airflow UI: http://localhost:8080
   - Trigger the `etl_pipeline` DAG

3. **Run data quality tests**
   ```bash
   cd firmable_dbt
   dbt test
   ```

4. **Test LLM entity matching**
   ```bash
   python llm_entity_matching_demo.py
   ```

## 🧪 Testing

### Unit Tests
```bash
pytest ETL_PIPELINE_AUS/scripts/tests/
```

### Integration Tests
```bash
# Test database connectivity
python -c "from sqlalchemy import create_engine; engine = create_engine('postgresql://postgres:password@localhost:5432/newdb'); print('Connection successful')"
```

### Data Quality Tests
```bash
cd firmable_dbt
dbt test
```

## 🔧 Configuration

### Database Configuration

Update database connection in scripts:
```python
DB_URI = "postgresql+psycopg2://postgres:your_password@localhost:5432/new_db"
```

### Airflow Configuration

Set Airflow variables in the UI or via CLI:
```bash
airflow variables set DB_PASSWORD your_password
airflow variables set OPENAI_API_KEY your_api_key
```

### dbt Configuration

Update `firmable_dbt/profiles.yml`:
```yaml
firmable_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: your_password
      port: 5432
      dbname: new_db
      schema: raw_data
```

## 📈 Performance Tuning

### For Large Datasets (>100k records)

1. **Increase batch size**: Set `BATCH_SIZE=5000` in environment
2. **Enable parallel processing**: Use ThreadPoolExecutor for matching
3. **Database optimization**: Add indexes and partitions
4. **Memory management**: Monitor and adjust Python memory limits

### Production Recommendations

1. **Resource limits**: Set Docker memory/CPU limits
2. **Monitoring**: Enable comprehensive logging and alerting
3. **Backup**: Set up automated database backups
4. **Security**: Use secrets management for API keys

## 🔍 Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Test connection
psql -U postgres -d newdb -c "SELECT 1;"
```

#### Airflow Issues
```bash
# Reset Airflow database
airflow db reset

# Check Airflow logs
tail -f ~/airflow/logs/dag_id=etl_pipeline/task_id=extract/run_id=*/attempt=1.log
```

#### LLM API Issues
```bash
# Test OpenAI API
curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models
```

### Performance Issues

#### Memory Problems
- Reduce batch size in processing scripts
- Monitor memory usage with `htop` or `docker stats`
- Consider using Spark for very large datasets

#### Slow Matching
- Enable blocking strategy in entity matching
- Use parallel processing with ThreadPoolExecutor
- Optimize database indexes

## 🤝 Contributing

### Development Workflow

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b feature/amazing-feature`
3. **Make** changes with tests
4. **Commit** with descriptive messages: `git commit -m "Add amazing feature"`
5. **Push** to your branch: `git push origin feature/amazing-feature`
6. **Submit** a pull request

### Code Standards

- **Python**: Follow PEP 8 style guide
- **SQL**: Use consistent formatting and naming conventions
- **Tests**: Maintain >80% code coverage
- **Documentation**: Update docs for new features

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

### Getting Help

- **Documentation**: Check this README and inline code comments
- **Issues**: Create GitHub issues for bugs or feature requests
- **Email**: Contact devyamehrotra123@gmail.com for urgent issues




## 🏆 Key Features

- ✅ **Multi-source data extraction** (ABR + Common Crawl)
- ✅ **Entity matching** (Fuzzy + TF-IDF + LLM)
- ✅ **Data quality assurance** (dbt models and tests)
- ✅ **Workflow orchestration** (Apache Airflow)
- ✅ **Database integration** (PostgreSQL with optimized loading)
- ✅ **LLM integration** (OpenAI GPT for advanced matching)
- ✅ **Environment-based configuration** (Secure credential management)

## 🎯 Use Cases

- **Business Intelligence**: Unified view of Australian companies
- **Data Enrichment**: Enhance company profiles with multiple sources
- **Entity Resolution**: Identify and merge duplicate company records
- **Market Analysis**: Comprehensive Australian business landscape data
- **Compliance**: Regulatory reporting and data validation 