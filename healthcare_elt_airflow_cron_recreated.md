# USE CASE: Healthcare Patient Analytics — ELT + Airflow

// Business Problem

A healthcare company receives:

* Patient visit data as JSON
* Patient master data from MySQL

Every night at 2:00 AM, the company wants to automatically:

1. Load raw JSON data into HDFS.
2. Import patient data from MySQL into HDFS.
3. Create Hive RAW tables.
4. Transform the data using Hive.
5. Generate patient-level analytics.
6. Display the complete pipeline and task status in Airflow.

//# Final result

```text
JSON + MySQL
     ↓
   HDFS RAW
     ↓
 Hive RAW Tables
     ↓
 Hive Transformation
     ↓
Patient Analytics
     ↓
   Airflow
     ↓
Daily 2:00 AM
```

---

# STEP 1 — CHECK HADOOP

Open WSL Ubuntu.

Check Java:

```bash
java -version
```

For your current Hadoop setup, you should see Java 8.

Check Hadoop:

```bash
hadoop version
```

Expected:

```text
Hadoop 3.3.6
```

Check HDFS:

```bash
jps
```

You should have at least:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

If Hadoop is not running, start it:

```bash
start-dfs.sh
start-yarn.sh
```

Check:

```bash
jps
```

---

# STEP 2 — CHECK HIVE

Run:

```bash
hive --version
```

You should get your installed Hive version.

Test:

```bash
hive
```

Inside Hive:

```sql
SHOW DATABASES;
```

Exit:

```sql
exit;
```

---

# STEP 3 — CHECK MYSQL

Install MySQL if required:

```bash
sudo apt update
sudo apt install mysql-server -y
```

Start it:

```bash
sudo service mysql start
```

Check:

```bash
sudo service mysql status
```

Enter MySQL:

```bash
sudo mysql
```

---

# STEP 4 — CREATE PROJECT DIRECTORY

Exit MySQL if you are still inside it:

```sql
exit;
```

Create our project:

```bash
mkdir -p ~/healthcare-elt/data
mkdir -p ~/healthcare-elt/sql
mkdir -p ~/healthcare-elt/scripts
mkdir -p ~/healthcare-elt/logs
```

Check:

```bash
tree ~/healthcare-elt
```

Expected:

```text
/home/sari/healthcare-elt
├── data
├── logs
├── scripts
└── sql
```

If `tree` is missing:

```bash
sudo apt install tree -y
```

---

# STEP 5 — CREATE JSON INPUT

Our first source is JSON.

Create:

```bash
nano ~/healthcare-elt/data/patient_visits.json
```

Paste:

```json
{"visit_id":"V001","patient_id":"P001","doctor":"Dr_Ravi","department":"Cardiology","diagnosis":"Hypertension","amount":5000,"visit_date":"2026-09-28"}
{"visit_id":"V002","patient_id":"P002","doctor":"Dr_Anita","department":"Neurology","diagnosis":"Migraine","amount":3000,"visit_date":"2026-09-28"}
{"visit_id":"V003","patient_id":"P001","doctor":"Dr_Ravi","department":"Cardiology","diagnosis":"Chest Pain","amount":7000,"visit_date":"2026-09-28"}
{"visit_id":"V004","patient_id":"P003","doctor":"Dr_Kiran","department":"Orthopedic","diagnosis":"Arthritis","amount":4500,"visit_date":"2026-09-28"}
{"visit_id":"V005","patient_id":"P004","doctor":"Dr_Anita","department":"Neurology","diagnosis":"Migraine","amount":2500,"visit_date":"2026-09-28"}
{"visit_id":"V006","patient_id":"P002","doctor":"Dr_Anita","department":"Neurology","diagnosis":"Headache","amount":3500,"visit_date":"2026-09-28"}
```

Save:

```text
Ctrl + O
Enter
Ctrl + X
```

Check:

```bash
cat ~/healthcare-elt/data/patient_visits.json
```

Expected:

```text
V001 P001 ...
V002 P002 ...
V003 P001 ...
V004 P003 ...
V005 P004 ...
V006 P002 ...
```

---

# STEP 6 — CREATE SQL INPUT

The second source is MySQL.

Create:

```bash
nano ~/healthcare-elt/sql/patients.sql
```

Paste:

```sql
CREATE DATABASE IF NOT EXISTS healthcare;

USE healthcare;

DROP TABLE IF EXISTS patients;

CREATE TABLE patients (
    patient_id VARCHAR(20),
    patient_name VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50)
);

INSERT INTO patients VALUES
('P001','John','Male',45,'Bangalore'),
('P002','Anita','Female',32,'Mysore'),
('P003','Ravi','Male',58,'Bangalore'),
('P004','Priya','Female',40,'Chennai');
```

Save.

---

# STEP 7 — LOAD SQL DATA INTO MYSQL

Run:

```bash
sudo mysql < ~/healthcare-elt/sql/patients.sql
```

Check:

```bash
sudo mysql -e "USE healthcare; SELECT * FROM patients;"
```

Expected:

```text
patient_id  patient_name  gender  age  city
P001        John          Male    45   Bangalore
P002        Anita         Female  32   Mysore
P003        Ravi          Male    58   Bangalore
P004        Priya         Female  40   Chennai
```

---

# STEP 8 — CREATE HDFS RAW FOLDERS

Create:

```bash
hdfs dfs -mkdir -p /healthcare-elt/raw/visits
hdfs dfs -mkdir -p /healthcare-elt/raw/patients
```

Check:

```bash
hdfs dfs -ls -R /healthcare-elt
```

Expected:

```text
/healthcare-elt/raw
/healthcare-elt/raw/visits
/healthcare-elt/raw/patients
```

---

# STEP 9 — LOAD JSON INTO HDFS

Run:

```bash
hdfs dfs -put -f \
~/healthcare-elt/data/patient_visits.json \
/healthcare-elt/raw/visits/
```

Check:

```bash
hdfs dfs -ls /healthcare-elt/raw/visits
```

Expected:

```text
patient_visits.json
```

Check contents:

```bash
hdfs dfs -cat /healthcare-elt/raw/visits/patient_visits.json
```

You should see the six JSON records.

---

# STEP 10 — CHECK SQOOP

Check:

```bash
sqoop version
```

If `sqoop` is not installed, install/configure it before continuing.

Since this is a Hadoop/Sqoop lab, make sure the MySQL JDBC driver is available to Sqoop.

Check:

```bash
ls /usr/share/java/ | grep mysql
```

You should have a MySQL connector JAR.

---

# STEP 11 — IMPORT MYSQL DATA INTO HDFS

Run:

```bash
sqoop import \
--connect jdbc:mysql://localhost:3306/healthcare \
--username root \
--password welcome \
--table patients \
--target-dir /healthcare-elt/raw/patients \
--m 1
```

If your MySQL root password is different, replace `welcome`.

If root has no password, use:

```bash
sqoop import \
--connect jdbc:mysql://localhost:3306/healthcare \
--username root \
--table patients \
--target-dir /healthcare-elt/raw/patients \
--m 1
```

Check:

```bash
hdfs dfs -ls /healthcare-elt/raw/patients
```

Expected:

```text
part-m-00000
```

Read it:

```bash
hdfs dfs -cat /healthcare-elt/raw/patients/part-m-00000
```

Expected:

```text
P001,John,Male,45,Bangalore
P002,Anita,Female,32,Mysore
P003,Ravi,Male,58,Bangalore
P004,Priya,Female,40,Chennai
```

At this point:

```text
JSON → HDFS
MySQL → Sqoop → HDFS
```

This is the LOAD part of ELT.
///////////////////////////////////////////////////

LOAD COMPLETED 




///////////////////////////


//////
HIVE WILL TAKE RAW DATA FROM HDFS INTO EXTERNAL TABLE OF HIVE.

//////////////////////////////
# STEP 12 — CREATE HIVE DATABASE

Start:

```bash
hive
```

Run:

```sql
CREATE DATABASE IF NOT EXISTS healthcare_elt;

USE healthcare_elt;
```

---

# STEP 13 — CREATE PATIENT RAW TABLE

Run:

```sql
CREATE EXTERNAL TABLE patients_raw (
    patient_id STRING,
    patient_name STRING,
    gender STRING,
    age INT,
    city STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/healthcare-elt/raw/patients';
```

Test:

```sql
SELECT * FROM patients_raw;
```

Expected:

```text
P001 John  Male   45 Bangalore
P002 Anita Female 32 Mysore
P003 Ravi  Male   58 Bangalore
P004 Priya Female 40 Chennai
```

---

# STEP 14 — CREATE JSON RAW TABLE

Our JSON is newline-delimited JSON.

Run:

```sql
CREATE EXTERNAL TABLE visits_raw (
    visit_id STRING,
    patient_id STRING,
    doctor STRING,
    department STRING,
    diagnosis STRING,
    amount DOUBLE,
    visit_date STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/healthcare-elt/raw/visits';
```

Test:

```sql
SELECT * FROM visits_raw;
```

Expected:

```text
V001 P001 Dr_Ravi  Cardiology  Hypertension  5000  2026-09-28
V002 P002 Dr_Anita Neurology   Migraine      3000  2026-09-28
V003 P001 Dr_Ravi  Cardiology  Chest Pain    7000  2026-09-28
V004 P003 Dr_Kiran Orthopedic  Arthritis     4500  2026-09-28
V005 P004 Dr_Anita Neurology   Migraine      2500  2026-09-28
V006 P002 Dr_Anita Neurology   Headache      3500  2026-09-28
```

If `JsonSerDe` is unavailable in your Hive installation, stop here and tell me the exact error; don't change the rest of the project yet.

---

# STEP 15 — CREATE FINAL ANALYTICS TABLE

Run:

```sql
CREATE TABLE patient_analytics (
    patient_id STRING,
    patient_name STRING,
    city STRING,
    total_visits INT,
    total_spent DOUBLE
)
STORED AS PARQUET;
```

---

# STEP 16 — PERFORM ELT TRANSFORMATION

This is where the T happens.

Run:

```sql
INSERT OVERWRITE TABLE patient_analytics
SELECT
    p.patient_id,
    p.patient_name,
    p.city,
    COUNT(v.visit_id) AS total_visits,
    SUM(v.amount) AS total_spent
FROM patients_raw p
JOIN visits_raw v
ON p.patient_id = v.patient_id
GROUP BY
    p.patient_id,
    p.patient_name,
    p.city;
```

Check:

```sql
SELECT * FROM patient_analytics;
```

Expected:

```text
P001  John   Bangalore  2  12000
P002  Anita  Mysore     2   6500
P003  Ravi   Bangalore  1   4500
P004  Priya  Chennai    1   2500
```

Exit:

```sql
exit;
```

---

# STEP 17 — CREATE HIVE TRANSFORMATION FILE

We now move the Hive transformation into a file so that Airflow can execute it automatically.

```bash
nano ~/healthcare-elt/scripts/transform_hive.sql
```

Paste:

```sql
USE healthcare_elt;

DROP TABLE IF EXISTS patient_analytics;

CREATE TABLE patient_analytics (
    patient_id STRING,
    patient_name STRING,
    city STRING,
    total_visits INT,
    total_spent DOUBLE
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE patient_analytics
SELECT
    p.patient_id,
    p.patient_name,
    p.city,
    COUNT(v.visit_id) AS total_visits,
    SUM(v.amount) AS total_spent
FROM patients_raw p
JOIN visits_raw v
ON p.patient_id = v.patient_id
GROUP BY
    p.patient_id,
    p.patient_name,
    p.city;
```

Save.

Test it:

```bash
hive -f ~/healthcare-elt/scripts/transform_hive.sql
```

Then:

```bash
hive -e "USE healthcare_elt; SELECT * FROM patient_analytics;"
```

Expected:

```text
P001    John    Bangalore    2    12000
P002    Anita   Mysore       2     6500
P003    Ravi    Bangalore    1     4500
P004    Priya   Chennai      1     2500
```

---

# STEP 18 — CREATE RAW LOAD SCRIPT

Create:

```bash
nano ~/healthcare-elt/scripts/load_raw.sh
```

Paste:

```bash
#!/bin/bash

echo "Starting RAW data load..."

echo "Loading JSON visits into HDFS..."

hdfs dfs -mkdir -p /healthcare-elt/raw/visits

hdfs dfs -put -f \
/home/sari/healthcare-elt/data/patient_visits.json \
/healthcare-elt/raw/visits/

echo "Importing patients from MySQL..."

hdfs dfs -rm -r -f /healthcare-elt/raw/patients

sqoop import \
--connect jdbc:mysql://localhost:3306/healthcare \
--username root \
--password welcome \
--table patients \
--target-dir /healthcare-elt/raw/patients \
--m 1

echo "RAW data load completed."
```

Save.

Make executable:

```bash
chmod +x ~/healthcare-elt/scripts/load_raw.sh
```

Test:

```bash
~/healthcare-elt/scripts/load_raw.sh
```

Expected:

```text
Starting RAW data load...
Loading JSON visits into HDFS...
Importing patients from MySQL...
RAW data load completed.
```

---

# STEP 19 — CREATE COMPLETE ELT SCRIPT

Create:

```bash
nano ~/healthcare-elt/scripts/run_pipeline.sh
```

Paste:

```bash
#!/bin/bash

LOG=/home/sari/healthcare-elt/logs/pipeline_$(date +%Y%m%d).log

echo "========================================" >> $LOG
echo "Healthcare ELT Pipeline Started" >> $LOG
echo "Start Time: $(date)" >> $LOG

echo "STEP 1: Loading RAW data" >> $LOG

/home/sari/healthcare-elt/scripts/load_raw.sh >> $LOG 2>&1

if [ $? -ne 0 ]; then
    echo "RAW LOAD FAILED" >> $LOG
    exit 1
fi

echo "STEP 2: Hive Transformation" >> $LOG

hive -f /home/sari/healthcare-elt/scripts/transform_hive.sql >> $LOG 2>&1

if [ $? -ne 0 ]; then
    echo "HIVE TRANSFORMATION FAILED" >> $LOG
    exit 1
fi

echo "STEP 3: Pipeline Completed" >> $LOG
echo "End Time: $(date)" >> $LOG
```

Save.

Make executable:

```bash
chmod +x ~/healthcare-elt/scripts/run_pipeline.sh
```

---

# STEP 20 — TEST COMPLETE ELT PIPELINE

Run:

```bash
~/healthcare-elt/scripts/run_pipeline.sh
```

Check:

```bash
cat ~/healthcare-elt/logs/pipeline_$(date +%Y%m%d).log
```

Expected:

```text
Healthcare ELT Pipeline Started
STEP 1: Loading RAW data
Loading JSON visits into HDFS...
Importing patients from MySQL...
RAW data load completed.
STEP 2: Hive Transformation
STEP 3: Pipeline Completed
```

Check final data:

```bash
hive -e "USE healthcare_elt; SELECT * FROM patient_analytics;"
```

Expected:

```text
P001    John    Bangalore    2    12000
P002    Anita   Mysore       2     6500
P003    Ravi    Bangalore    1     4500
P004    Priya   Chennai      1     2500
```

Now the ELT project is complete.

Only after this do we add Airflow.

---

# STEP 21 — INSTALL AIRFLOW

Install Python tools:

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv -y
```

Check:

```bash
python3 --version
```

Create Airflow environment:

```bash
python3 -m venv ~/airflow-venv
```

Activate:

```bash
source ~/airflow-venv/bin/activate
```

You should see:

```text
(airflow-venv)
```

Upgrade pip:

```bash
pip install --upgrade pip
```

---

# STEP 22 — SET AIRFLOW HOME

Run:

```bash
export AIRFLOW_HOME=$HOME/airflow
```

Make permanent:

```bash
nano ~/.bashrc
```

Add:

```bash
export AIRFLOW_HOME=$HOME/airflow
```

Save and reload:

```bash
source ~/.bashrc
```

Check:

```bash
echo $AIRFLOW_HOME
```

Expected:

```text
/home/sari/airflow
```

---

# STEP 23 — INSTALL AIRFLOW

Check Python:

```bash
python --version
```

For Python 3.12, for example:

```bash
AIRFLOW_VERSION=3.0.6
PYTHON_VERSION=3.12

pip install "apache-airflow==${AIRFLOW_VERSION}" \
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Then:

```bash
pip install apache-airflow-providers-standard
```

Check:

```bash
airflow version
```

Expected:

```text
3.0.6
```

The exact compatible version can differ depending on your Python version.

---

# STEP 24 — INITIALIZE AIRFLOW

Run:

```bash
airflow db migrate
```

Check:

```bash
ls ~/airflow
```

You should have:

```text
airflow.cfg
airflow.db
logs
```

---

# STEP 25 — CREATE AIRFLOW USER

Run:

```bash
airflow users create \
--username admin \
--firstname Sari \
--lastname Admin \
--role Admin \
--email admin@example.com \
--password admin
```

Use:

```text
Username: admin
Password: admin
```

for this local lab.

---

# STEP 26 — CREATE DAG DIRECTORY

```bash
mkdir -p ~/airflow/dags
```

Check:

```bash
ls ~/airflow
```

Expected:

```text
airflow.cfg
airflow.db
dags
logs
```

---


# STEP 26A — UNDERSTAND CRON SCHEDULING IN AIRFLOW (BEGINNER)

This is the part that controls **WHEN Airflow starts the DAG automatically**.

There are two different things that beginners often confuse:

### 1. Linux Cron

Linux has a separate scheduler called `cron`, configured with:

```bash
crontab -e
```

For example:

```text
0 2 * * *
```

would mean "run every day at 2:00 AM".

**Do NOT use Linux `crontab -e` for this Airflow project.**

### 2. Airflow Cron Schedule

Airflow itself accepts a cron expression in the DAG:

```python
schedule="0 2 * * *"
```

This is the cron-style schedule we use in this project.

The Airflow Scheduler reads this value and creates the DAG runs according to that schedule. In Airflow 3, the DAG parameter is `schedule`; older tutorials may show `schedule_interval`. For this project use `schedule`.

Official Airflow documentation confirms that a DAG can use a cron expression in its `schedule` argument. 

## STEP 26B — LEARN THE FIVE CRON FIELDS

The basic cron format is:

```text
MINUTE HOUR DAY-OF-MONTH MONTH DAY-OF-WEEK
```

Our expression is:

```text
0 2 * * *
```

Read it from left to right:

```text
0    = minute 0
2    = hour 2
*    = every day of the month
*    = every month
*    = every day of the week
```

Therefore:

```text
0 2 * * *
↓
Every day at 2:00 AM
```

## STEP 26C — VERY SIMPLE CRON EXAMPLES

| Cron expression | Meaning |
|---|---|
| `0 2 * * *` | Every day at 2:00 AM |
| `0 6 * * *` | Every day at 6:00 AM |
| `30 8 * * *` | Every day at 8:30 AM |
| `0 */2 * * *` | Every 2 hours |
| `0 9 * * 1-5` | Monday-Friday at 9:00 AM |
| `0 0 * * 0` | Every Sunday at midnight |
| `*/15 * * * *` | Every 15 minutes |
| `0 0 1 * *` | First day of every month at midnight |

For this healthcare project we use:

```python
schedule="0 2 * * *"
```

because the business requirement says:

```text
Run the healthcare ELT pipeline every night at 2:00 AM.
```

## STEP 26D — IMPORTANT: INDIA TIME (IST)

Airflow stores date/time information in UTC internally, and a fresh Airflow installation normally uses UTC as its default timezone.

Therefore, if the business requirement specifically means:

```text
2:00 AM India Standard Time (IST)
```

it is safer to make the DAG timezone-aware.

Use `pendulum`, which is installed as an Airflow dependency.

The DAG can use:

```python
import pendulum

IST = pendulum.timezone("Asia/Kolkata")
```

and:

```python
start_date=pendulum.datetime(2026, 9, 29, tz="Asia/Kolkata")
```

Then:

```python
schedule="0 2 * * *"
```

means 2:00 AM in the DAG's Asia/Kolkata timezone.

This is different from simply assuming that `2` always means India time.

For this lab, we will explicitly use **Asia/Kolkata** so that the teaching example is clear.

## STEP 26E — DO NOT WAIT UNTIL 2 AM WHILE TESTING

Suppose today is 10:30 AM.

If your DAG says:

```python
schedule="0 2 * * *"
```

Airflow will not immediately run it just because you created the DAG.

The automatic schedule is for the scheduled run at 2:00 AM.

For testing, use:

```text
Airflow UI
   ↓
Trigger DAG
   ↓
Run immediately
```

This lets you test the complete pipeline now.

So remember:

```text
Trigger DAG
    =
Manual test NOW

schedule="0 2 * * *"
    =
Automatic daily schedule
```

## STEP 26F — WHAT THE AIRFLOW SCHEDULER ACTUALLY DOES

The Scheduler is a continuously running Airflow service.

The simplified flow is:

```text
                    Airflow Scheduler
                           |
                           | checks DAG schedule
                           v
                    2:00 AM arrives
                           |
                           v
                 Create DAG Run
                           |
                           v
                    load_raw_data
                           |
                           v
                    transform_hive
                           |
                           v
                     check_output
```

The Scheduler does NOT replace the tasks.

It decides **when a DAG run should be created**.

The DAG defines **what tasks should execute and in what order**.

## STEP 26G — CHECK THE SCHEDULE IN THE DAG FILE

After creating the DAG, check the line:

```bash
grep -n "schedule" ~/airflow/dags/healthcare_elt_dag.py
```

Expected:

```text
schedule="0 2 * * *",
```

You can also inspect the complete file:

```bash
cat ~/airflow/dags/healthcare_elt_dag.py
```

## STEP 26H — TEST THE CRON EXPRESSION BEFORE USING IT

You can understand the expression manually:

```text
0 2 * * *
│ │ │ │ │
│ │ │ │ └── day of week = every day
│ │ │ └──── month = every month
│ │ └────── day of month = every day
│ └──────── hour = 2
└────────── minute = 0
```

So:

```text
0 2 * * *
```

is:

```text
02:00 every day
```

## STEP 26I — IMPORTANT AIRFLOW CONCEPT: SCHEDULE VS TRIGGER

### Scheduled run

Airflow Scheduler waits for the cron time:

```text
2:00 AM
   ↓
Scheduler
   ↓
DAG Run
```

### Manual run

You click:

```text
Trigger DAG
```

and Airflow starts a run immediately.

Therefore, a DAG can be:

```text
Scheduled automatically
        AND
Triggered manually for testing
```

Both are normal Airflow operations.

## STEP 26J — CHECK THAT THE SCHEDULER IS RUNNING

Open another WSL terminal and activate the environment:

```bash
source ~/airflow-venv/bin/activate
```

Set:

```bash
export AIRFLOW_HOME=$HOME/airflow
```

Start the scheduler:

```bash
airflow scheduler
```

Keep this terminal open.

The scheduler must be running for automatic scheduled DAG runs.

If the scheduler is stopped:

```text
DAG file exists
       +
schedule is correct
       +
Scheduler is stopped
       ↓
No automatic scheduled run
```

So for this local lab you normally keep these services running:

```text
Terminal 1
    airflow api-server --port 8080

Terminal 2
    airflow scheduler
```

## STEP 26K — BEGINNER CHECKLIST BEFORE EXPECTING A 2 AM RUN

Check all five:

```text
1. DAG file exists
   ~/airflow/dags/healthcare_elt_dag.py

2. DAG is visible
   airflow dags list

3. No import error
   airflow dags list-import-errors

4. DAG is unpaused / ON
   Airflow UI

5. Scheduler is running
   airflow scheduler
```

If all five are correct, Airflow can create scheduled DAG runs.

## STEP 26L — COMMON BEGINNER MISTAKES

### Mistake 1: Using Linux crontab

Do not do this for the Airflow DAG:

```bash
crontab -e
```

Airflow has its own scheduler.

### Mistake 2: Writing the wrong cron

Wrong:

```python
schedule="2 * * * *"
```

This means minute 2 of every hour, NOT 2:00 AM every day.

Correct:

```python
schedule="0 2 * * *"
```

### Mistake 3: Expecting the DAG to run immediately

A schedule does not mean "run now".

Use:

```text
Trigger DAG
```

for immediate testing.

### Mistake 4: Scheduler not running

The DAG may appear in the UI, but automatic scheduled runs require the Airflow Scheduler service.

### Mistake 5: Timezone confusion

If the business says "2:00 AM IST", explicitly use `Asia/Kolkata` in the DAG rather than assuming the Airflow server timezone is IST.


# STEP 27 — CREATE AIRFLOW DAG

This is the most important Airflow step.

Create:

```bash
nano ~/airflow/dags/healthcare_elt_dag.py
```

Paste:

```python
import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


IST = pendulum.timezone("Asia/Kolkata")


with DAG(
    dag_id="healthcare_elt_pipeline",
    start_date=pendulum.datetime(2026, 9, 29, tz="Asia/Kolkata"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["healthcare", "elt", "hadoop", "hive"],
) as dag:

    load_raw_data = BashOperator(
        task_id="load_raw_data",
        bash_command="/home/sari/healthcare-elt/scripts/load_raw.sh",
    )

    transform_hive = BashOperator(
        task_id="transform_hive",
        bash_command="hive -f /home/sari/healthcare-elt/scripts/transform_hive.sql",
    )

    check_output = BashOperator(
        task_id="check_output",
        bash_command=(
            "hive -e "
            "\"USE healthcare_elt; "
            "SELECT * FROM patient_analytics;\""
        ),
    )

    load_raw_data >> transform_hive >> check_output
```

Save:

```text
Ctrl + O
Enter
Ctrl + X
```

---

# STEP 28 — CHECK DAG FOR ERRORS

Run:

```bash
airflow dags list
```

Look for:

```text
healthcare_elt_pipeline
```

Then:

```bash
airflow dags list-import-errors
```

There should be no error for our DAG.

Check tasks:

```bash
airflow tasks list healthcare_elt_pipeline
```

Expected:

```text
check_output
load_raw_data
transform_hive
```

---

# STEP 29 — START AIRFLOW API SERVER

Open Terminal 1.

Activate:

```bash
source ~/airflow-venv/bin/activate
```

Run:

```bash
airflow api-server --port 8080
```

Keep this terminal open.

---

# STEP 30 — START AIRFLOW SCHEDULER

Open Terminal 2.

Activate:

```bash
source ~/airflow-venv/bin/activate
```

Set:

```bash
export AIRFLOW_HOME=$HOME/airflow
```

Start:

```bash
airflow scheduler
```

Keep this terminal open.

Now:

```text
Terminal 1
    |
    +-- Airflow API Server

Terminal 2
    |
    +-- Airflow Scheduler
```

---

# STEP 31 — OPEN AIRFLOW UI

Open your Windows browser.

Go to:

```text
http://localhost:8080
```

You should see the Airflow login screen.

Login:

```text
Username: admin
Password: admin
```

---

# STEP 32 — FIND YOUR DAG

In the Airflow UI, find:

```text
healthcare_elt_pipeline
```

You should see:

```text
healthcare_elt_pipeline
```

Turn the DAG ON/unpaused.

---

# STEP 33 — OPEN THE DAG

Click:

```text
healthcare_elt_pipeline
```

You should see the tasks:

```text
load_raw_data
       |
       v
transform_hive
       |
       v
check_output
```

This is the Airflow representation of our ELT pipeline.

---

# STEP 34 — RUN DAG NOW

Do not wait until 2 AM.

In the Airflow UI, click:

```text
Trigger DAG
```

This starts the pipeline immediately.

You should see the DAG run appear.

---

# STEP 35 — WATCH TASK 1

Task:

```text
load_raw_data
```

It executes:

```bash
/home/sari/healthcare-elt/scripts/load_raw.sh
```

That performs:

```text
JSON
 ↓
HDFS

MySQL
 ↓
Sqoop
 ↓
HDFS
```

Open the task logs.

You should see:

```text
Starting RAW data load...
Loading JSON visits into HDFS...
Importing patients from MySQL...
RAW data load completed.
```

Task becomes:

```text
SUCCESS
```

---

# STEP 36 — WATCH TASK 2

Next:

```text
transform_hive
```

It executes:

```bash
hive -f /home/sari/healthcare-elt/scripts/transform_hive.sql
```

This performs:

```text
patients_raw
       +
visits_raw
       ↓
     JOIN
       ↓
patient_analytics
```

Expected result:

```text
P001 John   Bangalore  2 12000
P002 Anita  Mysore     2  6500
P003 Ravi   Bangalore  1  4500
P004 Priya  Chennai    1  2500
```

Task:

```text
SUCCESS
```

---

# STEP 37 — WATCH TASK 3

Next:

```text
check_output
```

It executes:

```sql
SELECT * FROM patient_analytics;
```

Expected Airflow log:

```text
P001    John    Bangalore    2    12000
P002    Anita   Mysore       2     6500
P003    Ravi    Bangalore    1     4500
P004    Priya   Chennai      1     2500
```

Task:

```text
SUCCESS
```

---

# STEP 38 — FINAL AIRFLOW UI RESULT

Your DAG should finally show:

```text
healthcare_elt_pipeline

        SUCCESS
           |
           v
+------------------+
|  load_raw_data   |
|     SUCCESS      |
+------------------+
           |
           v
+------------------+
| transform_hive   |
|     SUCCESS      |
+------------------+
           |
           v
+------------------+
|  check_output    |
|     SUCCESS      |
+------------------+
```

---

# STEP 39 — VERIFY FROM TERMINAL

Run:

```bash
hive -e "USE healthcare_elt; SELECT * FROM patient_analytics;"
```

Expected:

```text
P001    John    Bangalore    2    12000
P002    Anita   Mysore       2     6500
P003    Ravi    Bangalore    1     4500
P004    Priya   Chennai      1     2500
```

---

# STEP 40 — UNDERSTAND THE 2 AM SCHEDULE

Inside the DAG we wrote:

```python
schedule="0 2 * * *"
```

This means:

```text
Minute = 0
Hour   = 2
Day    = *
Month  = *
Week   = *
```

Therefore:

```text
Every day at 2:00 AM
```

The Airflow scheduler continuously checks the schedule.

At 2:00 AM:

```text
Airflow Scheduler
       |
       v
healthcare_elt_pipeline
       |
       v
load_raw_data
       |
       v
transform_hive
       |
       v
check_output
```

---

# STEP 41 — MANUAL VS AUTOMATIC RUN

For your students, this distinction is important.

//# Manual testing

From UI:

```text
Trigger DAG
```

means:

```text
RUN NOW
```

//# Production schedule

```python
schedule="0 2 * * *"
```

means:

```text
RUN AUTOMATICALLY EVERY DAY AT 2 AM
```

So you can test the DAG immediately without waiting for 2 AM.

---


# STEP 41A — CRON CHEAT SHEET FOR STUDENTS

Remember this simple rule:

```text
Airflow schedule
       ↓
Cron expression
       ↓
Scheduler reads it
       ↓
DAG Run is created
       ↓
Tasks execute
```

For this project:

```python
schedule="0 2 * * *"
```

means:

```text
Every day
    at
2:00 AM
Asia/Kolkata
```

### Useful schedules

```python
schedule="@daily"
```

Every day at midnight according to the DAG timezone.

```python
schedule="0 2 * * *"
```

Every day at 2:00 AM.

```python
schedule="0 6 * * *"
```

Every day at 6:00 AM.

```python
schedule="*/15 * * * *"
```

Every 15 minutes.

```python
schedule="0 9 * * 1-5"
```

Monday to Friday at 9:00 AM.

### The three commands students should remember

Check the DAG:

```bash
airflow dags list
```

Check import errors:

```bash
airflow dags list-import-errors
```

List tasks:

```bash
airflow tasks list healthcare_elt_pipeline
```

Then use the UI:

```text
Trigger DAG
```

to test immediately.

### Interview explanation

If an interviewer asks:

**"How did you schedule the Airflow pipeline?"**

Answer:

> "I used Airflow's cron-based `schedule` parameter. In our healthcare ELT project, `schedule="0 2 * * *"` schedules the DAG to run every day at 2:00 AM. The Airflow Scheduler continuously evaluates the DAG schedule and creates the DAG run. For testing, I can manually trigger the DAG from the Airflow UI without waiting for the scheduled time. I also configured the DAG timezone as Asia/Kolkata because the business requirement is 2:00 AM IST."


# STEP 42 — FINAL PROJECT STRUCTURE

// Local Linux

```text
/home/sari/
│
├── healthcare-elt/
│   │
│   ├── data/
│   │   └── patient_visits.json
│   │
│   ├── sql/
│   │   └── patients.sql
│   │
│   ├── scripts/
│   │   ├── load_raw.sh
│   │   ├── transform_hive.sql
│   │   └── run_pipeline.sh
│   │
│   └── logs/
│
├── airflow/
│   │
│   ├── dags/
│   │   └── healthcare_elt_dag.py
│   │
│   ├── logs/
│   ├── airflow.cfg
│   └── airflow.db
│
└── airflow-venv/
```

// HDFS

```text
/healthcare-elt/
│
└── raw/
    │
    ├── visits/
    │   └── patient_visits.json
    │
    └── patients/
        └── part-m-00000
```

// Hive

```text
healthcare_elt
│
├── patients_raw
├── visits_raw
└── patient_analytics
```

---

# COMPLETE FLOW FOR YOUR PRACTICAL

```text
                 DATA SOURCES
                     |
            +--------+--------+
            |                 |
          JSON              MySQL
            |                 |
            |               Sqoop
            |                 |
            +--------+--------+
                     |
                     v
                    HDFS
                 RAW LAYER
                     |
          +----------+----------+
          |                     |
          v                     v
    patients_raw           visits_raw
          |                     |
          +----------+----------+
                     |
                     v
                Hive JOIN
                     |
                     v
             patient_analytics
                  PARQUET
                     ^
                     |
                 AIRFLOW
                     |
                Every 2 AM
                     |
                     v
            healthcare_elt_dag
                     |
          +----------+----------+
          |          |          |
          v          v          v
        Load      Transform   Check
         RAW         Hive     Output
```

// One-line project explanation

> “We implemented an automated healthcare ELT pipeline where JSON and MySQL data are first loaded into HDFS as raw data, Hive performs the transformation to generate patient analytics, and Apache Airflow orchestrates the pipeline automatically every day at 2 AM.”

Important for your actual lab: run these steps in order. If you get an error at any step—especially Sqoop, Hive JSON SerDe, or Airflow installation—stop at that step and send me the exact terminal output. We can fix that step before proceeding.


# AIRFLOW SCHEDULING REFERENCE

The cron scheduling and timezone explanation in this revised document was aligned with the Apache Airflow documentation:
- Cron expressions are supported through the DAG `schedule` argument.
- Airflow supports timezone-aware DAG schedules.
- A default Airflow installation uses UTC unless the timezone is configured otherwise.

For this lab, the DAG explicitly uses `Asia/Kolkata` so the stated 2:00 AM requirement is unambiguous.
