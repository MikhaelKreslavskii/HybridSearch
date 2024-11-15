import json
from pprint import pprint
import os
import time

from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
from langchain_community.embeddings import HuggingFaceEmbeddings, GigaChatEmbeddings
class Search:

    def __init__(self):
        self.model = GigaChatEmbeddings(
    credentials="", scope="GIGACHAT_API_PERS", verify_ssl_certs=False
    )
        self.es = Elasticsearch("http://localhost:9200/")  # <-- connection options need to be added here
        client_info = self.es.info()
        print('Connected to Elasticsearch!')
        pprint(client_info.body)

    def create_index(self):
        self.es.indices.delete(index='my_documents', ignore_unavailable=True)
        self.es.indices.create(index='my_documents', mappings={
            'properties': {
                'embedding': {
                    'type': 'dense_vector',
                    # "dims": 1024,

                }
            }
        })

    def get_embedding(self, text):
        return self.model.embed_query(text)
    def insert_document(self, document):
        print(self.es.index(index='my_documents', body=document)['_id'])
        return self.es.index(index='my_documents', document={
            **document,
            'embedding': self.get_embedding(document['refs']),
        })

    def insert_documents(self, documents):
        operations = []
        for document in documents:
            operations.append({'index': {'_index': 'my_documents'}})
            operations.append(document)
        return self.es.bulk(operations=operations)

    def search(self, **query_args):
        return self.es.search(index='my_documents', **query_args)

    def retrieve_document(self, id):
        return self.es.get(index='my_documents', id=id)

    def reindex(self):
        self.create_index()
        with open('kb.json', 'rt', encoding='utf-8') as f:
            documents = json.loads(f.read())
        for document in documents:
            self.insert_document(document)
        # return self.insert_documents(documents)