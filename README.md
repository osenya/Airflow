Weather ETL Pipeline using Apache Airflow, Python & Amazon S3
This project demonstrates the implementation of a modern Extract, Transform, Load (ETL) pipeline using Apache Airflow. The pipeline fetches real-time weather data from the OpenWeatherMap API, transforms it into a clean format, and uploads it directly to an Amazon S3 bucket, automating the end-to-end data flow using Airflow DAGs.
Key Components
•	Data Source: OpenWeatherMap API providing live weather metrics.
•	Scheduler: Apache Airflow orchestrates the workflow using a Directed Acyclic Graph (DAG).
•	Storage: Processed data is saved directly as a .csv file in Amazon S3 — no local disk retention.
Pipeline Breakdown
1.	Extract
A PythonOperator task retrieves live weather data for Nairobi using the OpenWeatherMap API and stores it temporarily in Airflow’s XCom system for downstream tasks.
2.	Transform
The data is parsed to extract relevant metrics such as:
•	Timestamp
•	Temperature
•	Humidity
•	Weather description
The cleaned data is converted to a Pandas DataFrame.
3.	Load
The transformed data is serialized to a temporary CSV file and uploaded directly to Amazon S3 using the S3Hook, placing the file under a structured folder path like weather_data/YYYYMMDD_HHMM.csv.
 
 Technologies Used
Tool / Service	Purpose
Apache Airflow	Workflow orchestration
Python & Pandas	Data extraction and transformation
Amazon S3	Cloud-based storage destination
S3Hook	Airflow utility for S3 uploads
XComs	Inter-task communication in DAGs
OpenWeatherMap API	Real-time data feed
 
 Highlights
•	Modular and Scalable: Each task in the DAG is reusable and independently testable.
•	Cloud-First Storage: Entire flow is serverless-friendly and avoids local disk storage.
•	Airflow Best Practices: Uses XComs, retries, and dynamic filenames for robust automation.
•	Environment Agnostic: Can run in any Airflow environment (local, Docker, MWAA, Astronomer).


