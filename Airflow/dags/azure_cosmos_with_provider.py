from airflow import DAG
from airflow.providers.microsoft.azure.hooks.cosmos import AzureCosmosDBHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 18),
}

dag = DAG(
    'azure_cosmos_hook_example',
    default_args=default_args,
    description='Example DAG that uses Azure Cosmos DB hook',
    schedule_interval=None,
)

cosmos_hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_default')
# Example query
query = 'SELECT * FROM c'

client = cosmos_hook.get_conn()
database_name = 'my_database'
container_name = 'my_container'

database_link = f"/dbs/{database_name}"
container_link = f"{database_link}/colls/{container_name}"

container_proxy = client.get_container_client(container_link)

query_results = container_proxy.query_items(query=query, enable_cross_partition_query=True)

for item in query_results:
    print(item)