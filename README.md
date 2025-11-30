### DataEng2025 Project - Doris Käämbre, August Roosi, Anton Katsuba, Rasmus Mirma

# Impact of Weather on Estonian Parliamentary Sittings

## Setup Instructions

### 1. Clone the repository
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Modify values in `.env` as needed

### 3. Start Core Services (Airflow + ClickHouse + dbt)
```bash
docker compose up -d
```

This starts the essential services:
- **Airflow** (webserver, scheduler, postgres) - for orchestrating data pipelines
- **ClickHouse** - data warehouse
- **dbt** - data transformations
- **pgAdmin** - database management UI

### 4. Access Airflow and Run DAGs
Open http://localhost:8080 (credentials: `airflow` / `airflow`)

Run DAGs in this order:
1. `parliamentary_ingestion_dag` - ingests parliament voting data
2. `weather_ingestion_dag` - ingests weather data  
3. `transformation_dag` - runs dbt transformations (requires dbt sources to be configured)

### 5. Start Superset for Data Visualization
```bash
docker compose --profile superset up -d
```

### 6. Start OpenMetadata for Data Catalog
```bash
docker compose --profile openmetadata up -d
```

### 7. Initialize Metadata
```bash
./scripts/init_metadata.sh --all
```

---

## Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| ClickHouse | http://localhost:8123 | `clickhouse` / `clickhouse` |
| pgAdmin | http://localhost:5050 | `admin@example.com` / `admin` |
| Superset | http://localhost:8088 | `admin` / `admin` |
| OpenMetadata | http://localhost:8585 | `admin` / `admin` |

---

## Clickhouse users:
Username: user_full, password: user_full
Username: user_limited, password: user_limited


## Superset - Connect to ClickHouse
1. Go to **Settings → Database Connections → + Database**
2. Select **ClickHouse Connect**
3. In the bottom, select and Use SQLAlchemy URI: `clickhousedb://clickhouse:clickhouse@clickhouse-server:8123/gold`

## OpenMetadata - Register ClickHouse Tables
1. Go to **Settings → Services → Databases → Add New Service**
2. Select **ClickHouse** and configure:
   - Host: `clickhouse-server`
   - Port: `8123`
   - Username: `clickhouse`
   - Database: `gold`
3. Run metadata ingestion to discover tables


---

## Pre-populate Metadata

To automatically set up database connections, table descriptions, and glossary terms, run these initialization scripts after all services are healthy:

```bash
# Install Python requests library if not already available
pip3 install requests

# Initialize OpenMetadata with table descriptions and glossary
python3 openmetadata/init/init_openmetadata.py

# Initialize Superset with ClickHouse connection and datasets
python3 superset/assets/init_superset.py
```

This will create:
- **OpenMetadata**: Database service, table/column descriptions, and business glossary
- **Superset**: ClickHouse database connection and dataset registrations

| Script | What it creates |
|--------|-----------------|
| `init_openmetadata.py` | `clickhouse-riigikogu` service, `gold`/`parliament_data`/`weather_data` databases, table descriptions, "Riigikogu Data Glossary", data quality tests |
| `init_superset.py` | `ClickHouse Riigikogu` database connection, `Voting`, `DimWeather`, `DimDate`, `DimVotingType` datasets |

---

## Data Quality Tests

The project includes automated data quality tests defined in OpenMetadata. After running the initialization script, the following tests are available:

| Test Name | Type | Table | Column | Description |
|-----------|------|-------|--------|-------------|
| `voting_weatherid_not_null` | NOT NULL | FactVoting | WeatherId | Ensures the foreign key to DimWeather is never null |
| `voting_type_fk_not_null` | NOT NULL | FactVoting | VotingType | Ensures the foreign key to DimVotingType is never null |
| `dimdate_date_unique` | UNIQUE | DimDate | Date | Ensures date dimension surrogate key is unique |
| `voting_present_count_valid` | VALUE RANGE | FactVoting | Present | Ensures attendee count is between 0-101 (Riigikogu has 101 members) |

### Data Quality Tests Screenshot

![OpenMetadata Quality Tests](openmetadata_quality.png)

---

## 1. Business Brief

The objective of the project is to collect and process the historic data of attendance in Estonian parliamentary sittings and analyse possible correlation between the weather data and the attendance. 

The stakeholders would be Estonian citizens that gain visibility on how environmental factors may influence parliamentary participation and decision-making.  
The policymakers could use the finding to evaluate the attendance dynamics, optimize sitting schedules or identify patterns in decision-making. 

---

### Key Metrics (KPIs)

1. **Attendance rate per sitting**  
   Indicates overall parliamentary participation.  
   **Formula:** `Attendees / Total registered members of parliament`

2. **Attendance rate by political party**  
   Indicates internal differences across political groups.  
   **Formula:** `Number of present members of parliament from faction / Total number of members of parliament from faction`

3. **Consensus rate**  
   Measure of agreement in the parliament during a vote. High consensus rate indicates alignment, low indicates division.  
   **Formula:** `Max(Yes, No, Abstained, Neutral vote count) / Total votes`

---

### Business Questions

1. How does **precipitation** affect attendance or voting consensus?  
   - Is the attendance rate smaller if the precipitation is high?  
   - Are some political groups more affected than others?  

2. How does **temperature** affect attendance or voting consensus?  
   - Is the consensus rate higher if the temperature is higher?  

3. How does **cloudiness** affect attendance or voting consensus?  

4. How does **wind** affect attendance or voting consensus?  

5. How does the **day of the week** affect attendance or voting consensus?  

---

### Datasets

- **Riigikogu API:** [https://www.riigikogu.ee/en/open-data/](https://www.riigikogu.ee/en/open-data/)  
- **Estonian Environment Agency:**  
  - [Weather data (XML feed)](https://www.ilmateenistus.ee/teenused/ilmainfo/eesti-vaatlusandmed-xml/)  
  - [Historical weather data](https://www.ilmateenistus.ee/kliima/ajaloolised-ilmaandmed/)  

---

## 3. Tooling

| Purpose | Tools |
|----------|-------|
| Storage | PostgreSQL |
| Transformation | dbt |
| Ingestion | Docker, Airflow |
| Serving | ClickHouse, Open Metadata |

---

## 4. Data Architecture

<img width="580" height="652" alt="image" src="https://github.com/user-attachments/assets/03ad6263-9ecc-4fc6-918a-fb6611897d9e" />


### Data Quality Checks
- Each `VotingId` should have **101 records** in `FactVotingMember` table.  
- **Uniqueness checks** for primary keys of each table:  
  - `VotingId` in `FactVoting`  
  - `WeatherId` in `FactWeather`  
  - `VotingId + MemberId` in `FactVotingMember`  
  - `SittingId` in `DimSitting`  
  - `MemberId` in `DimMember`  
  - `Date` in `DimDate`  
- No overlapping validities per `MemberId` in `DimMember` table
  - Check that one member (SrcMemberId) would not have records that have overlapping time validities (columns `ValidFrom`, `ValidTo`).  

---

## 5/6. Data Model & Dictionary

### Granularity
- **FactVotingMember:** One record per voting and member  
- **FactVoting:** One record per voting  
- **FactWeather:** One record per hour  

### Slowly Changing Dimensions
| Table | Type | Description |
|--------|------|-------------|
| `DimMember` | Type 2 | Stores each parliamentary member with political party affiliations |
| `DimSitting` | Static | Sitting details such as date, start/end time and type of sitting |
| `DimDate` | Static | Calendar table that contains dates and specifies whether it is a holiday |


<img width="3447" height="3840" alt="schema" src="https://github.com/user-attachments/assets/2f0a1702-8dd3-4457-b718-8c2936534ad6" />


**Facts**
- `FactVotingMember`: Information about members and their vote for each voting that takes place
- `FactWeather`: Each row describes the weather of a single hour from each day. Contains precipitation, temperature and cloud coverage.
- `FactVoting`: Records information about each voting session. Columns capture the time of the vote, type of the vote, description, attendee counts and end results of the vote.


[**Demo Queries**](demo_queries.sql)


## 7. Airflow DAGs
Examples of executed DAGs can be seen on pictures below.
<img width="1920" height="951" alt="dag1" src="https://github.com/user-attachments/assets/ce1b50dc-36de-43c1-b874-ee32c1814af5" />
<img width="1920" height="951" alt="dag2" src="https://github.com/user-attachments/assets/6707714f-5ebd-458a-bf00-c74061f797ae" />


## 8. Permission issues
If you have error related to permission issue, execute the `perm.sh` script with sudo. 

