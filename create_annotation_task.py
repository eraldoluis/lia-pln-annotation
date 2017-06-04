from datetime import datetime

from dateutil import tz
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import scan


def checkIndexAndType(es, index, docType):
    """
    Check if the given index and type exist and if the corresponding mapping is correct.
    If the doc type or the index do not exist, create them.

    :param es: Elasticsearch client.
    :param index:
    :param docType:
    :return:
    """
    ic = IndicesClient(es)
    if not ic.exists(index=index):
        ic.create(index=index)

    if not ic.exists_type(index=index, doc_type=docType):
        # Create type.
        ic.put_mapping(index=index, doc_type=docType, body={
            "properties": {}
        })

    mapping = ic.get_mapping(index=index, doc_type=docType)
    properties = mapping[index]["mappings"][docType]
    if len(properties) == 0:
        ic.put_mapping(index=index, doc_type=docType, body={
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "created": {
                    "type": "date"
                },
                "docId": {
                    "type": "keyword"
                },
                "numValidAnnotations": {
                    "type": "long"
                },
                "annotations": {
                    "properties": {
                        "annotatorId": {
                            "type": "keyword"
                        },
                        "annotation": {
                            "type": "keyword"
                        },
                        "time": {
                            "type": "date"
                        }
                    }
                },
                "invalid": {
                    "properties": {
                        "annotatorId": {
                            "type": "keyword"
                        },
                        "cause": {
                            "type": "text"
                        },
                        "time": {
                            "type": "date"
                        }
                    }
                }
            }
        })

    return True


def create_annotation_task(es, originIndex, originType, targetIndex, targetType, name, query, numberOfDocs=None):
    checkIndexAndType(es, index=targetIndex, docType=targetType)

    created = datetime.now(tz.tzlocal())

    if numberOfDocs is None:
        numberOfDocs = float('inf')
    count = 0
    for doc in scan(client=es, index=originIndex, doc_type=originType, query=query):
        annDoc = {
            "name": name,
            "created": created,
            "docId": doc["_id"],
            "doc": doc["_source"]
        }

        es.index(index=targetIndex, doc_type=targetType, body=annDoc)

        count += 1

        if count >= numberOfDocs:
            break


if __name__ == "__main__":
    create_annotation_task(Elasticsearch(['http://localhost:9200']), originIndex="ctrls", originType="twitter",
                           targetIndex="ctrls_annotation", targetType="annotation_relevance", name="supernatural",
                           query={
                               "query": {
                                   "bool": {
                                       "filter": [
                                           {
                                               "term": {
                                                   "start": "2017-02-20T16:33:25.093458-04:00"
                                               }
                                           }
                                       ]
                                   }
                               }
                           },
                           numberOfDocs=10)
