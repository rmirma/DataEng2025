### DataEng2025 Project - Doris Käämbre, August Roosi, Anton Katsuba, Rasmus Mirma

# Impact of Weather on Estonian Parliamentary Sittings

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

<img width="752" height="205" alt="diagram" src="https://github.com/user-attachments/assets/c667090a-0e19-4cce-8975-e1ef71044653" />

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
