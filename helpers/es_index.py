import boto3
from elasticsearch import Elasticsearch
import pandas as pd

es = Elasticsearch(
    hosts=[{'host': 'search-restaurants-data-muort2nl3f5mctqi6272obu7e4.aos.us-east-1.on.aws', 'port': 443}],
    http_auth=('', ''),
    scheme="https",
    port=443,
)

health = es.cluster.health()
print(health)

response = es.indices.create(index="restaurants", body={
    "mappings": {
        "properties": {
            "restaurant_id": {"type": "text"},
            "cuisine": {"type": "text"}
        }
    }
})

print(response)
df = pd.read_csv('results.csv')

documents = df[['restaurant_id', 'cuisine']].to_dict(orient='records')


def bulk_index(es_client, index_name, doc_type, documents):
    bulk_data = []
    for doc in documents:
        bulk_data.append({
            '_op_type': 'index',
            '_index': index_name,
            #'_type': doc_type,
            '_source': doc
        })
    from elasticsearch import helpers
    helpers.bulk(es_client, bulk_data)


index_name = 'restaurants'
doc_type = 'Restaurant'

# Bulk index the documents into Elasticsearch
bulk_index(es, index_name, doc_type, documents)

