# Commands for Clean Presentation

## 1. Clean and Setup Everything (Run before starting the recording)

First, build the latest version of the code just to be safe:
```bash
mvn clean package -DskipTests
```

Next, clear out any lingering temporary batch files from previous tests:
```bash
rm -rf /tmp/etl_* /tmp/hive_*
```

Finally, completely reset the MySQL database to delete all your test data. This uses the new command we just built. It will prompt you to type 'yes' to confirm:
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db --reset
```

*(You are now at a completely clean slate. Start your video recording now.)*

---

## 2. Presentation Demo Commands (Run during the recording)

Run these exactly in this order to perfectly demonstrate all the tool's capabilities for your evaluator, as per the guidelines.

**Step 1: Show Help (Proves it's a unified CLI tool)**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar help
```

**Step 2: Initialize MySQL**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar init-db
```

**Step 3: Run all pipelines with file-based batching (Fastest way to get data)**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query all --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mapreduce --query all --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline pig --query all --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline hive --query all --batch-strategy file_based
```

**Step 3: Run single queries individually (Shows you can target specific queries)**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query query1 --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mapreduce --query query2 --batch-strategy file_based
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline pig --query query3 --batch-strategy file_based
```

**Step 4: List all runs (Shows the table of the 7 runs you just did)**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar list-runs
```

**Step 5: View the detailed report for the Pig pipeline**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar report --latest --pipeline pig
```

**Step 6: Compare pipelines (Shows runtime ranking & result consistency)**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar compare --batch-strategy file_based
```

**Step 7: Show record-count batching (Demonstrates the batch progress UI)**
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar run --pipeline mongodb --query all --batch-strategy record_count --batch-size 500000
```

**Step 8: Show Database tables in MySQL**
Open a new terminal window and run:
```bash
sudo mysql -u etl_user -p etl_results
```
Then run these queries to show the raw data is actually there for all queries:
```sql
SELECT * FROM run_metadata;
SELECT * FROM query1_daily_traffic LIMIT 5;
SELECT * FROM query2_top_resources LIMIT 5;
SELECT * FROM query3_hourly_errors LIMIT 5;
```

**Step 9: Interactive Mode**
Go back to your main terminal and run:
```bash
java -jar target/etl-nasa-logs-1.0-SNAPSHOT.jar
```
*(In interactive mode, just walk through the menu options to show the evaluator that the tool has a guided, user-friendly interface.)*
