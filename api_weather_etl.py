from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd

# -----------------------------------------------------------------------------------------------------
# Extract data from OpenWeatherMap API
# ----------------------------------------------------------------------------------------------------
def extract_weather_data(ti):
    print("Extracting data from OpenWeatherMap API")
    api_key = Variable.get("OWM_API_KEY")
    city = "Nairobi"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    ti.xcom_push(key="weather_raw", value=data)

# -----------------------------------------------------------------------------------------------------
# Transform data
# ----------------------------------------------------------------------------------------------------
def transform_weather_data(ti):
    print('Transforming data')
    data = ti.xcom_pull(task_ids="extract_task", key="weather_raw")

    df = pd.DataFrame([{
        "timestamp": datetime.utcnow().isoformat(),
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather": data["weather"][0]["description"]
    }])

    ti.xcom_push(key="weather_df", value=df.to_dict())

# -----------------------------------------------------------------------------------------------------
# Load data to S3
# ----------------------------------------------------------------------------------------------------
def load_weather_data(ti):
    print("Loading data")

    weather_dict = ti.xcom_pull(task_ids="transform_weather_task", key="weather_df")
    df = pd.DataFrame.from_dict(weather_dict)

    file_name = f"weather_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
    file_path = f"/tmp/{file_name}"
    df.to_csv(file_path, index=False)

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    bucket_name = Variable.get("S3_BUCKET_NAME")
    s3_key = f"weather_data/{file_name}"

    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

    print(f"✅ Uploaded {file_name} to s3://{bucket_name}/{s3_key}")

# -----------------------------------------------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------------------------------------------
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="weather_etl_upgraded",
    start_date=datetime(2025, 6, 25),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    description="Extract weather data and upload to S3"
)

extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_weather_data,
        dag=dag
    )

transform_task = PythonOperator(
        task_id="transform_weather_task",
        python_callable=transform_weather_data,
        dag=dag
    )

load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_weather_data,
        dag=dag
    )

extract_task >> transform_task >> load_task
# End of DAG definition