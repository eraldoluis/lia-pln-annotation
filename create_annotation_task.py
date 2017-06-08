from datetime import datetime
from sys import stdout

from dateutil import tz
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import scan


def checkIndexAndType(es, index, docType):
    """
    Check if the given index and type exist. If the doc type or the index do not exist, create them and the
    corresponding mappings.

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


def create_annotation_task(es, index, docType, name, sourceIndex, sourceType, query, numberOfDocs=float('inf')):
    """
    Create an annotation item for each doc in the given query.

    :param es:
    :param index:
    :param docType:
    :param name:
    :param query:
    :param numberOfDocs:
    :return:
    """
    checkIndexAndType(es, index=index, docType=docType)

    created = datetime.now(tz.tzlocal())

    count = 0
    for doc in scan(es, index=sourceIndex, doc_type=sourceType, query=query):
        annDoc = {
            "name": name,
            "created": created,
            "docId": doc["_id"],
            "doc": doc["_source"]
        }

        es.index(index=index, doc_type=docType, body=annDoc)

        count += 1

        if count % 10000 == 0:
            stdout.write('.')
            stdout.flush()

        if count >= numberOfDocs:
            break

    stdout.write('\n')
    print 'Created %d items' % count


def main():
    es = Elasticsearch(['http://localhost:9200'])

    checkIndexAndType(es, index="ctrls_annotation", docType="relevance")

    # created = datetime.now(tz.tzlocal())
    #
    # for i in xrange(100):
    #     annDoc = {
    #         "name": "teste",
    #         "created": created,
    #         "docId": i,
    #         "doc": "Doc %d" % i
    #     }
    #     es.index(index="test_annotation_index", doc_type="test_annotation", body=annDoc)

    create_annotation_task(es, index="ctrls_annotation", docType="relevance", name="supernatural",
                           sourceIndex="ctrls", sourceType="twitter",
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
                           })


if __name__ == "__main__":
    main()
