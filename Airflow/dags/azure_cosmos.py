from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import azure.cosmos.cosmos_client as cosmos_client
def read_cosmosdb_to_dataframe(conn_id, database_name, container_name):
    HOST = "https://tranvietcuonghust.documents.azure.com:443/"
    MASTER_KEY = "Bx8F7H61NwRqoXyjPBeqaejtzXO3gZXWePCEqpOI4awtCg1KqosrmnhlRNC687WxGHSodF2f9VFWACDbl6jGxg=="
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
    database = client.get_database_client(database_name)
    container = database.get_container_client(container_name)
    query = "SELECT * FROM c WHERE c.Name = @name"
    params = [{"name": "@name", "value": "Dell Latitude 5300 i5-8365U 8GB SSD 240GB/256GB 13.3\" FHD IPS Touch"}]

# Execute the query
    items = list(container.query_items(query, parameters=params))

# Print the item if found
    if items:
        print(items[0])
    else:
        print("Item not found")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 18),
}

dag = DAG(
    'read_cosmosdb_to_dataframe',
    default_args=default_args,
    description='Read items from Azure Cosmos DB into a Pandas dataframe',
    schedule_interval=None,
)

read_cosmosdb_task = PythonOperator(
    task_id='read_cosmosdb',
    python_callable=read_cosmosdb_to_dataframe,
    op_kwargs={
        'conn_id': 'azure_cosmos_default',
        'database_name': 'test_Database',
        'container_name': 'test_Container'
    },
    dag=dag,
)

read_cosmosdb_task