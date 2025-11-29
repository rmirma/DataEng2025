### DataEng2025 Project - Doris Käämbre, August Roosi, Anton Katsuba, Rasmus Mirma

# Impact of Weather on Estonian Parliamentary Sittings

## Setup Instructions

1. **Clone the repository**  
   ```bash
   git clone <repository-url>
   cd DataEng2025
   ```

2. **Create environment file:**
   ```bash
   cp .env.example .env
   ```

3. **Build and start the Docker containers:**    
   ```bash
   docker compose up --build -d
   ```
   This will automatically initialize all databases and services.

4. **Run dbt to create gold layer models:**
   ```bash
   docker run --rm --network dataeng2025_default -v "${PWD}/dbt_project:/dbt" -w /dbt python:3.9-slim bash -c "pip install --quiet dbt-clickhouse && dbt run --profiles-dir /dbt"
   ```

5. **Trigger the DAGs:**  
   In the Airflow UI, locate and trigger the DAGs to start data ingestion.

6. **Access the services:**  

   | Service | URL | Credentials |
   |---------|-----|-------------|
   | Airflow | http://localhost:8080 | `airflow` / `airflow` |
   | Superset | http://localhost:8088 | `admin` / `admin` |
   | OpenMetadata | http://localhost:8585 | `admin` / `admin` |
   | ClickHouse | http://localhost:8123 | `default` / (empty) |
   | pgAdmin | http://localhost:5050 | `admin@example.com` / `admin` |

### Superset - Connect to ClickHouse
1. Go to **Settings → Database Connections → + Database**
2. Select **ClickHouse Connect**
3. Use SQLAlchemy URI: `clickhousedb://default@clickhouse-server:8123/default_gold`

### OpenMetadata - Register ClickHouse Tables
1. Go to **Settings → Services → Databases → Add New Service**
2. Select **ClickHouse** and configure:
   - Host: `clickhouse-server`
   - Port: `8123`
   - Username: `default`
   - Database: `default_gold`
3. Run metadata ingestion to discover tables



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
| Storage | PostgreSQL (Airflow metadata), ClickHouse (data warehouse) |
| Transformation | dbt |
| Ingestion | Docker, Airflow |
| Visualization | Apache Superset |
| Metadata Management | OpenMetadata |
| Serving | ClickHouse |

---

## 4. Data Architecture

The project follows a medallion architecture with bronze, silver (not implemented), and gold layers.

- **Bronze Layer**: Raw data ingestion from APIs and files into ClickHouse tables (voting, weather).
- **Gold Layer**: Transformed and aggregated data using dbt, including fact and dimension tables with data quality tests.
- **Metadata Management**: Tables registered in OpenMetadata for governance and discovery.

<img width="580" height="652" alt="image" src="https://github.com/user-attachments/assets/03ad6263-9ecc-4fc6-918a-fb6611897d9e" />

### Data Quality Checks
- **dbt Tests**: Implemented in `dbt_project/models/gold/schema.yml`
  - Not null on foreign keys in fact table (VotingType, WeatherId, Date in FactVoting)
  - Unique on surrogate keys in dimension tables (Date, VotingType, WeatherId)
  - Additional not null test on VotingId
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

### Gold Layer Tables
- **FactVoting**: Fact table of all votings with aggregated voting results and foreign keys to dimensions.
- **DimWeather**: Dimension table of weather data aggregated over 6 hours before each voting.
- **DimDate**: Dimension table for dates including holidays and seasons.
- **DimVotingType**: Dimension table for voting types.

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

## 8. dbt Models and Data Quality

The project uses dbt for data transformation and testing. Models are in `dbt_project/models/` with bronze and gold layers.

### Gold Layer Tables
| Table | Description |
|-------|-------------|
| `FactVoting` | Central fact table with voting sessions, attendance counts, and results |
| `DimWeather` | Weather conditions aggregated over 6 hours before each voting |
| `DimDate` | Calendar dimension with weekdays, holidays, and seasons |
| `DimVotingType` | Voting type categories |

### Data Quality Tests (in `schema.yml`)
- **Not null** on foreign keys: `VotingType`, `WeatherId`, `Date` in FactVoting
- **Unique** on surrogate keys: `VotingId`, `WeatherId`, `Date`, `VotingType`
- **Relationships** tests validating foreign key integrity
- **Not null** on `Present` column (attendance count)

Run tests:
```bash
docker run --rm --network dataeng2025_default -v "${PWD}/dbt_project:/dbt" -w /dbt python:3.9-slim bash -c "pip install --quiet dbt-clickhouse && dbt test --profiles-dir /dbt"
```

## 9. OpenMetadata

OpenMetadata provides metadata management and data discovery. Access at http://localhost:8585.

The ingestion service runs automatically and can be configured via the UI to discover ClickHouse tables.

## 10. Superset Dashboard

Apache Superset provides interactive dashboards for visualizing voting and weather data.

Access at http://localhost:8088 (credentials: `admin` / `admin`).

### Creating Charts
Example charts to answer business questions:
- **Temperature vs Attendance**: Bar chart showing average attendance by temperature range
- **Voting by Day of Week**: Pie chart showing voting distribution across weekdays
- **Weather Impact on Consensus**: Line chart comparing weather metrics to consensus rate

## 11. Troubleshooting

### Permission Issues (Linux/WSL)
If you encounter permission errors, run:
```bash
chmod +x perm.sh && sudo ./perm.sh
```

### Docker Issues
If Docker Desktop has issues, restart it or run:
```bash
docker compose down
docker compose up -d
``` 

