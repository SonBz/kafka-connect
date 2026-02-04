# Spring Boot Kafka Streams + Debezium Oracle Demo

This project demonstrates an End-to-End CDC (Change Data Capture) pipeline using:
- **Oracle Database 23c Free** (Source & Sink)
- **Kafka Connect** + **Debezium Oracle Connector**
- **Kafka KRaft** (No Zookeeper)
- **Spring Boot 3 + Kafka Streams** (Processing)

## Architecture

1.  **CDC**: Debezium captures changes in `CUSTOMER`, `ORDERS`, `PAYMENT` tables.
2.  **Streaming**: Spring Boot App joins streams:
    - `ORDERS` join `CUSTOMER` -> `Enriched Order`
    - `PAYMENT` join `Enriched Order` -> `Order Paid Event`
    - Aggregation -> `Daily Revenue`
3.  **Sink**: `Daily Revenue` is written back to Oracle table `REVENUE_REPORT`.

## Prerequisites

- Docker & Docker Compose
- Java 21+ (for running the app, or run inside container if desired)
- Maven (to build the app)

## Setup & Run

### 1. Infrastructure (Docker)

Start the infrastructure (Oracle, Kafka, Connect, UI):

```bash
docker compose up -d --build
```

**Wait (~2-5 mins)** for Oracle to be healthy (Status: healthy) and Kafka Connect to start.

### 2. Register Debezium Connector

Once Kafka Connect is up (check port 8083), register the connector:

```bash
cd connect
./register-connector.sh
```

Check status:
```bash
curl http://localhost:8083/connectors/oracle-connector/status
```
If it fails with "Archive Log" errors, ensure Oracle is properly configured (the scripts try to handle this but sometimes a restart is needed for ARCHIVELOG mode).
*Note*: The provided init scripts attempt to set up permissions for `C##DBZUSER`.

### 3. Run Spring Boot App

```bash
cd spring-boot-streams-app
./mvnw spring-boot:run
# or if you have maven installed:
mvn spring-boot:run
```

### 4. Demo Scenarios

#### View Data in Kafka UI
Go to [http://localhost:8080](http://localhost:8080). You should see topics like `oracle.MYUSER.CUSTOMER`.

#### Trigger Events in Oracle
Connect to Oracle (e.g., using DBeaver or SQLPlus inside docker):

```bash
docker exec -it oracle sqlplus myuser/mypassword@//localhost:1521/free
```

**Scenario 1: New Order**
```sql
INSERT INTO ORDERS (customer_id, amount, status) VALUES (1, 150.00, 'PENDING');
COMMIT;
```
*Check `oracle.MYUSER.ORDERS` topic.*

**Scenario 2: Pay for Order**
```sql
-- Find ID of inserted order first, assume 4
INSERT INTO PAYMENT (order_id, method, paid_amount) VALUES (4, 'PAYPAL', 150.00);
COMMIT;
```
*Check output topics:*
- `events.order_paid` -> Should show event with email "alice@example.com" (enriched).
- `analytics.daily_revenue` -> Should update revenue.

**Scenario 3: Verify Persistence**
```sql
SELECT * FROM REVENUE_REPORT;
```

## Troubleshooting

- **Oracle CDC Error**: `ORA-01291: missing logfile`.  
  *Fix*: Ensure `ARCHIVELOG` is enabled. 
  Check: `SELECT log_mode FROM v$database;`
  Enable:
  ```sql
  SHUTDOWN IMMEDIATE;
  STARTUP MOUNT;
  ALTER DATABASE ARCHIVELOG;
  ALTER DATABASE OPEN;
  ```
- **Connector Failures**: Check logs `docker logs connect`.

## Project Structure
- `docker-compose.yml`: Infra definition.
- `connect/`: Debezium config & build.
- `oracle/`: Init scripts.
- `spring-boot-streams-app/`: Java Application.
