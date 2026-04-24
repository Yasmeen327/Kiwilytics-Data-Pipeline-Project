from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
import pandas as pd
import matplotlib.pyplot as plt

PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

#TASK 1 FETCH DATA
def fetch_order_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()

    query = """
    SELECT 
        o.OrderDate::date AS sale_date,
        od.ProductID,
        p.ProductName,
        od.Quantity,
        p.Price
    FROM orders o
    JOIN order_details od ON o.OrderID = od.OrderID
    JOIN products p ON od.ProductID = p.ProductID
    """

    df = pd.read_sql(query, conn)
    df.to_csv('/home/kiwilytics/airflow_output/daily_sales_data.csv', index=False)

#PROCESS DAILY REVENUE

def process_daily_revenue():
    df = pd.read_csv('/home/kiwilytics/airflow_output/daily_sales_data.csv')

    df['total_revenue'] = df['Quantity'] * df['Price']

    revenue_per_day = df.groupby('sale_date').agg(
        total_revenue=('total_revenue', 'sum')
    ).reset_index()

    revenue_per_day.to_csv('/home/kiwilytics/airflow_output/daily_revenue.csv', index=False)

# PLOTTING

def plot_daily_revenue():
    df = pd.read_csv('/home/kiwilytics/airflow_output/daily_revenue.csv')

    df['sale_date'] = pd.to_datetime(df['sale_date'])

    plt.figure(figsize=(12, 6))
    plt.plot(df['sale_date'], df['total_revenue'], marker='o')

    plt.title("Daily Total Sales Revenue")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.grid(True)

    output_path = '/home/kiwilytics/airflow_output/daily_revenue_plot.png'
    plt.savefig(output_path)

    print(f"Saved to {output_path}")

    #CREATE THE DAG
with DAG(
    dag_id='daily_sales_revenue_analysis',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_fetch_data = PythonOperator(
        task_id='fetch_order_data',
        python_callable=fetch_order_data,
    )

    task_process_revenue = PythonOperator(
        task_id='process_daily_revenue',
        python_callable=process_daily_revenue,
    )

    task_plot_revenue = PythonOperator(
        task_id='plot_daily_revenue',
        python_callable=plot_daily_revenue,
    )

    task_fetch_data >> task_process_revenue >> task_plot_revenue
