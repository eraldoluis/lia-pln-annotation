#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
from sys import stdout

from dateutil import tz
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk
from elasticsearch.helpers import scan

#
# List of contexts for this task.
#
contexts = [
    {
        "name": "sao paulo",
        "terms": [u"são paulo", u"sao paulo"],
        "description": u"São Paulo Futebol Clube"
    },
    {
        "name": "santos",
        "terms": [u"santos"],
        "description": u"Santos Futebol Clube"
    },
    {
        "name": "bahia",
        "terms": [u"bahia"],
        "description": u"Esporte Clube Bahia"
    }
]


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
                "context": {
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "terms": {
                            "type": "keyword"
                        },
                        "description": {
                            "type": "text"
                        }
                    }
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
    :param sourceType:
    :param sourceIndex:
    :param query:
    :param numberOfDocs:
    :return:
    """
    checkIndexAndType(es, index=index, docType=docType)

    created = datetime.now(tz.tzlocal())

    def generator():
        count = 0
        for doc in scan(es, index=sourceIndex, doc_type=sourceType, query=query):
            # Analyse context.
            for context in contexts:
                text = doc["_source"]["tweet"]["text"].lower()
                if any([text.find(t) != -1 for t in context["terms"]]):
                    annDoc = {
                        "name": name,
                        "created": created,
                        "docId": doc["_id"],
                        "doc": doc["_source"],
                        "context": context
                    }

                    # print json.dumps(annDoc, indent=2)

                    action = {
                        '_op_type': 'index',
                        '_index': index,
                        '_type': docType,
                        '_source': annDoc
                    }

                    yield action

                    # es.index(index=index, doc_type=docType, body=annDoc)

            count += 1

            if count % 10000 == 0:
                stdout.write('.')
                stdout.flush()

            if count >= numberOfDocs:
                break

        stdout.write('\n')
        print 'Created %d items' % count

    bulk(es, generator())


def main():
    es = Elasticsearch(['http://localhost:9200'])

    targetIndex = "ctrls_annotation_no_retweet"
    targetType = "relevance"

    checkIndexAndType(es, index=targetIndex, docType=targetType)

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

    # create_annotation_task(es, index="ctrls_annotation", docType="relevance", name="futebol",
    #                        sourceIndex="ctrls", sourceType="twitter",
    #                        query={
    #                            "query": {
    #                                "bool": {
    #                                    "filter": [
    #                                        {
    #                                            "term": {
    #                                                "start": "2017-02-20T16:33:30.542448-04:00"  # FUTEBOL
    #                                            }
    #                                        }
    #                                    ]
    #                                }
    #                            }
    #                        },
    #                        numberOfDocs=20)

    create_annotation_task(es, index=targetIndex, docType=targetType, name="futebol",
                           sourceIndex="ctrls_no_retweet", sourceType="twitter",
                           query={
                               "query": {
                                   "bool": {
                                       "filter": [
                                           {
                                               "term": {
                                                   "start": "2017-02-20T16:33:30.542448-04:00"  # FUTEBOL
                                               }
                                           }
                                       ]
                                   }
                               }
                           })


if __name__ == "__main__":
    main()
