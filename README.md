# Firmable Australia ETL Pipeline

A comprehensive data pipeline for extracting, transforming, and integrating Australian company data from Common Crawl and the Australian Business Register (ABR). This solution demonstrates advanced entity matching using NLP, fuzzy logic, and LLM technologies with production-ready orchestration using Apache Airflow.

## 🎯 Project Overview

This ETL pipeline processes large-scale Australian business data through a multi-stage approach:
- **Extraction**: Downloads and parses ABR XML data and Common Crawl datasets
- **Transformation**: Cleans, normalizes, and standardizes company information
- **Loading**: Efficiently loads data into PostgreSQL with optimized indexing
- **Matching**: Performs entity matching using multiple algorithms (Fuzzy, TF-IDF, LLM)
- **Quality Assurance**: Implements comprehensive data quality testing with dbt

## 🏗️ Architecture

```mermaid
flowchart TD
    subgraph "Data Sources"
        DS1[ABR XML Data<br/>Australian Business Register]
        DS2[Common Crawl Data<br/>Web Crawled Companies]
    end
    
    subgraph "Extraction Layer"
        E1[ABR XML Extractor<br/>extract_abr_xml.py]
        E2[Common Crawl Extractor<br/>extract_common_crawl.py]
    end
    
    subgraph "Processing Layer"
        P1[Data Cleaning<br/>clean_abr.py & clean_common_crawl.py]
        P2[Schema Creation<br/>createschema.py]
        P3[Data Loading<br/>loadingcsv_topostgre_fast.py]
    end
    
    subgraph "Storage Layer"
        S1[PostgreSQL Raw Data<br/>raw_data schema]
        S2[Processed Data<br/>processed_data schema]
    end
    
    subgraph "Matching Engine"
        M1[Fuzzy Matching<br/>rapidfuzz]
        M2[TF-IDF Matching<br/>scikit-learn]
        M3[LLM Matching<br/>OpenAI GPT]
    end
    
    subgraph "Orchestration"
        O1[Apache Airflow<br/>etl_pipeline.py DAG]
        O2[dbt Models<br/>firmable_dbt/]
        O3[Performance Monitoring<br/>logs/]
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
    O1 --> E1
    O1 --> E2
    O1 --> P1
    O1 --> P2
    O1 --> P3
    O2 --> S2
    O3 --> O1
```

## 📁 Project Structure

```
FirmableProjectsAustrlia/
├── ETL_PIPELINE_AUS/                    # Main ETL pipeline directory
│   ├── scripts/                         # Core ETL scripts
│   │   ├── extraction/                  # Data extraction modules
│   │   │   ├── extract_abr_xml.py      # ABR XML parser
│   │   │   └── extract_common_crawl.py # Common Crawl extractor
│   │   ├── cleaning/                    # Data cleaning and normalization
│   │   │   ├── clean_abr.py            # ABR data cleaner
│   │   │   └── clean_common_crawl.py   # Common Crawl cleaner
│   │   ├── loading/                     # Database loading scripts
│   │   │   ├── loadingcsv_topostgre_fast.py      # Fast CSV loader
│   │   │   └── loadingcsv_topostgre_optimized.py # Optimized loader
│   │   ├── matching/                    # Entity matching algorithms
│   │   │   ├── entity_matching.py      # Basic fuzzy matching
│   │   │   ├── entity_matching_tfidf.py # TF-IDF matching
│   │   │   ├── entity_matching_spark.py # Spark-based matching
│   │   │   └── entity_matching_optimized.py # Optimized matching
│   │   ├── tests/                       # Unit and integration tests
│   │   │   └── test_etl_pipeline.py    # Pipeline tests
│   │   ├── utils/                       # Utility functions
│   │   └── createschema.py              # Database schema creation
│   ├── data/                            # Data files and directories
│   │   ├── abr/                         # ABR data
│   │   │   ├── raw/                     # Raw XML and CSV files
│   │   │   └── cleaned/                 # Cleaned data files
│   │   ├── common_crawl/                # Common Crawl data
│   │   │   ├── raw/                     # Raw crawl data
│   │   │   └── cleaned/                 # Cleaned crawl data
│   │   └── entity_matching/             # Entity matching results
│   ├── sql/                             # Database schema and DDL
│   │   └── schema_postgres.sql          # PostgreSQL schema
│   ├── firmable_dbt/                    # dbt models and tests
│   │   ├── models/                      # dbt transformation models
│   │   └── dbt_project.yml              # dbt project configuration
│   ├── logs/                            # Pipeline execution logs
│   ├── docs/                            # Documentation
│   ├── tests/                           # Additional tests
│   ├── requirements.txt                 # Python dependencies
│   └── README.md                        # Detailed documentation
├── dags/                                # Airflow DAGs
│   └── etl_pipeline.py                  # Main ETL pipeline DAG
├── LLM/                                 # LLM integration
│   └── llm_entity_matching_demo.py      # LLM matching demonstration
├── main.py                              # Entry point script
├── .gitignore                           # Git ignore rules
└── README.md                            # This file
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

## 🤖 Entity Matching Strategy

### Multi-Stage Matching Approach

1. **Exact Match**: ABN or website URL exact matches
2. **Fuzzy Match**: Company names with >85% similarity using rapidfuzz
3. **TF-IDF Match**: Natural language processing for semantic similarity
4. **LLM-Enhanced Match**: OpenAI GPT for complex business logic and reasoning

### Available Matching Algorithms

- **`entity_matching.py`**: Basic fuzzy matching with nested loops
- **`entity_matching_tfidf.py`**: TF-IDF vectorization with cosine similarity
- **`entity_matching_spark.py`**: Spark-based distributed matching
- **`entity_matching_optimized.py`**: Optimized matching with blocking and parallel processing

### LLM Integration

The pipeline includes LLM-based entity matching using OpenAI GPT:

```python
# Example LLM matching prompt
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

## 🛠️ Technology Stack

### Core Technologies

- **Python 3.12**: Primary development language
- **PostgreSQL**: Robust relational database with advanced indexing
- **Apache Airflow**: Workflow orchestration and scheduling
- **dbt**: Data transformation and quality testing
- **SQLAlchemy**: Database ORM and connection management

### Data Processing Libraries

- **pandas**: Data manipulation and analysis
- **rapidfuzz**: Fast fuzzy string matching
- **scikit-learn**: TF-IDF vectorization and similarity
- **OpenAI GPT**: Advanced entity matching and reasoning
- **psycopg2**: PostgreSQL adapter

### Development & Testing

- **pytest**: Unit and integration testing
- **pytest-mock**: Mocking for isolated testing
- **Git**: Version control and collaboration

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
   cd ETL_PIPELINE_AUS/firmable_dbt
   dbt test
   ```

4. **Test LLM entity matching**
   ```bash
   python ETL_PIPELINE_AUS/LLM/llm_entity_matching_demo.py
   ```

## 📊 Performance Optimizations

### Optimized Components

1. **Entity Matching**: 
   - Blocking strategy reduces O(n²) to O(n log n)
   - Parallel processing with ThreadPoolExecutor
   - Memory-efficient batch processing

2. **Data Loading**:
   - Connection pooling with SQLAlchemy
   - Fast COPY method for small files
   - Parallel batch loading for large files
   - Automatic index creation

3. **XML Processing**:
   - Streaming XML parsing
   - Memory-efficient processing
   - Error handling and recovery

### Performance Monitoring

- **Logging**: Comprehensive logging in `ETL_PIPELINE_AUS/logs/`
- **Metrics**: Processing time and throughput tracking
- **Error Handling**: Graceful failure recovery

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
cd ETL_PIPELINE_AUS/firmable_dbt
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

Update `ETL_PIPELINE_AUS/firmable_dbt/profiles.yml`:
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

### Community

- **GitHub Discussions**: Use GitHub Discussions for questions
- **Contributing**: See CONTRIBUTING.md for guidelines

---

**Made with ❤️ for the Australian business community**

## 🏆 Key Features

- ✅ **Multi-source data extraction** (ABR + Common Crawl)
- ✅ **Advanced entity matching** (Fuzzy + TF-IDF + LLM)
- ✅ **Data quality assurance** (dbt + comprehensive testing)
- ✅ **Production-ready orchestration** (Airflow + monitoring)
- ✅ **Scalable architecture** (PostgreSQL + optimized processing)
- ✅ **LLM integration** (OpenAI GPT for intelligent matching)
- ✅ **Performance optimizations** (Blocking, parallel processing, connection pooling)

## 🎯 Use Cases

- **Business Intelligence**: Unified view of Australian companies
- **Data Enrichment**: Enhance company profiles with multiple sources
- **Entity Resolution**: Identify and merge duplicate company records
- **Market Analysis**: Comprehensive Australian business landscape data
- **Compliance**: Regulatory reporting and data validation 