# coding=utf-8
import datetime
from threading import Thread, Condition

from dateutil import tz
from elasticsearch.helpers import scan

from annotated_item import AnnotatedItem


class AnnotationManager(Thread):
    """
    The annotation manager is responsible to keep a list of available tweets to be annotated. It also manages
    which tweet is associated to each user of the system, and saves the given annotations to ES.

    The annotation manager object is a singleton within the application, i.e., there is only one object that is
    shared by all requests/users.
    """

    def __init__(self, name, esClient, index, annotatorType, annotationType, esFilter, numAnnotationsPerItem, logger):
        """
        Create a new annotation manager object and spawn a new thread to produce new annotation items.

        :param name: friendly, but unique, name for this manager.
        :param esClient: Elasticsearch client.
        :param index: index in ES to be used.
        :param annotatorType: document type for annotators (users).
        :param annotationType: document type for annotations (tweets).
        :param esFilter: filter dict to use within the ES query to retrieve items to be annotated.
        :param numAnnotationsPerItem: number of annotations to be collected for each item.
        :param logger: logger object.
        """
        super(AnnotationManager, self).__init__(name="AnnotationManager-%s" % name)

        self.name = name
        self.es = esClient
        self.index = index
        self.annotatorType = annotatorType
        self.annotationType = annotationType
        self.filter = esFilter
        self.numAnnotationsPerItem = numAnnotationsPerItem
        self.logger = logger

        # Condition variable used to coordinate the producer (the manager thread) and the consumers (request threads).
        self.__condition = Condition()

        # Size of the queue of unannotated items. The producer thread will keep this number of items always available.
        self.numUnannotatedItems = 10

        # List of unannotated items retrieved and, thus, available to be annotated by any annotator.
        self.unannotatedItems = []

        # List of items that have been annotated by some annotators but that has not yet been annotated by the required
        # number of annotators (self.numAnnotationsPerItem).
        self.partiallyAnnotatedItems = []

        # These are items sent to annotators (responding to requests) but not yet annotated by them.
        self.mapAnnotatorItem = {}

        self.searchFrom = 0

        self.running = False

        # Spawn a new thread.
        # TODO: check if we can call this method from the constructor.
        self.start()

    def run(self):
        """
        Keep self.numUnannotatedItems in the self.unannotatedItems list. This thread is notified by the cosumers
        every time one item is removed from this list.
        :return:
        """
        self.running = True
        with self.__condition:
            self.__fillPartiallyAnnotatedItems()
            while self.running:
                lenUn = len(self.unannotatedItems)
                if lenUn < self.numUnannotatedItems:
                    self.__fillUnannotatedItems()

                    # Notify consumers if the list was empty.
                    if lenUn == 0 and len(self.unannotatedItems) > 0:
                        self.__condition.notifyAll()

                self.wait()

    def stop(self):
        self.running = False
        with self.__condition:
            self.__condition.notifyAll()

    def getItem(self, annotatorId):
        """
        Return the item associated with the given annotator (annotatorId) or, in case this annotator is not holding
        any item, get a new item to be annotated.

        :param annotatorId:
        :return:
        """
        with self.__condition:
            # Check if the annotator is holding some item.
            if annotatorId in self.mapAnnotatorItem:
                (itemId, item) = self.mapAnnotatorItem[annotatorId]
                # Update the obtained time for this item.
                item.holding[annotatorId]["time"] = datetime.now(tz.tzlocal())
                return itemId, item.doc

            return self.__nextItem(annotatorId)

    def annotate(self, annotatorId, itemId, annotation, invalidate=False):
        """
        Save the given annotation and return a new associated item.
        If invalidate is True, then annotation must be a message describing the cause to invalidate this item;
        and the item will be invalidate and never returned to any other annotator.

        :param annotatorId:
        :param itemId:
        :param annotation: valid values are 'yes', 'no' and None.
        :param invalidate: if True, invalidate this item and never return it again for any annotator.
            In this case, the annotation is a message describing the cause of the invalidation.
        :return: a new associated tweet.
        """
        with self.__condition:
            # Check if the annotator is holding some item.
            if annotatorId not in self.mapAnnotatorItem:
                self.logger.error(
                    "Annotator %s tried to annotate item %s but s/he was not holding this item. Getting a new one." % (
                        annotatorId, itemId))
                # Annotator was not holding any item. Get a new item.
                return self.__nextItem(annotatorId)

            # Check if the annotator is holding the given item.
            (itemId, item) = self.mapAnnotatorItem.get(annotatorId)
            if annotatorId not in item.holding:
                self.logger.error(
                    "Annotator %s tried to annotate item %s but s/he was not holding this item. Getting a new one." % (
                        annotatorId, itemId))
            else:
                if invalidate:
                    item.invalid = {
                        "annotatorId": annotatorId,
                        "cause": annotation,
                        "time": datetime.now(tz.tzlocal())
                    }
                else:
                    # Append the given annotation.
                    item.annotations[annotatorId] = {
                        "annotation": annotation,
                        "time": datetime.now(tz.tzlocal())
                    }

                    if annotation in ("yes", "no"):
                        item.numValidAnnotations += 1

                # Update Elasticsearch.
                self.es.update(index=self.index, doc_type=self.annotationType, id=item.id,
                               body={"doc": item.getSourceToUpdate()})

                # Remove annotator from the item's holding dictionary.
                del item.holding[annotatorId]

            # Unlink item and annotator.
            del self.mapAnnotatorItem[annotatorId]

            return self.__nextItem(annotatorId)

    def __fillPartiallyAnnotatedItems(self):
        """
        Fill the self.partiallyAnnotatedItems list with all items from Elasticsearch that includes some annotation but
        not the required number (self.numAnnotationsPerItem).
        """
        _scan = scan(self.es, index=self.index, doc_type=self.annotationType, query={
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "numValidAnnotations": {
                                    "lt": self.numAnnotationsPerItem,
                                    "gt": 0
                                }
                            }
                        }
                    ],
                    "must_not": {
                        "exists": {
                            "field": "invalid"
                        }
                    }
                }
            }
        })

        self.partiallyAnnotatedItems = [(item["_id"], item["_source"]) for item in _scan]

    def __fillUnannotatedItems(self):
        """
        Fill the list of unannotated items with the next items.
        These items have not been annotated by any annotator.
        """
        # Number of unannotated items to retrieve in order to fill the list.
        n = self.numUnannotatedItems - len(self.unannotatedItems)

        # Search n new items from the previous point (self.searchFrom).
        res = self.es.search(index=self.index, doc_type=self.annotationType, body={
            "from": self.searchFrom,
            "size": n,
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "filter": [
                                self.filter
                            ],
                            "must_not": {
                                "bool": {
                                    "should": [
                                        {
                                            "exists": {
                                                "field": "annotations"
                                            }
                                        },
                                        {
                                            "exists": {
                                                "field": "invalid"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    },
                    "random_score": {
                        "seed": 13
                    },
                    "boost_mode": "replace"
                }
            }
        })

        hits = res["hits"]["hits"]
        hits = [hit["_source"] for hit in hits]

        # Update from index.
        self.searchFrom += len(hits)

        # Append the retrieved items to the list.
        self.unannotatedItems += [(item["_id"], item["_source"]) for item in hits]

    def __nextItem(self, annotatorId):
        """
        Get a new item to be annotated by the given annotator.

        First, check if there is an item within the self.partiallyAnnotatedItems.
        If there is not, then get an unannotated item.

        :param annotatorId:
        :return:
        """
        # Look for partially annotated items.
        for i in xrange(len(self.partiallyAnnotatedItems)):
            (itemId, item) = self.partiallyAnnotatedItems[i]
            if annotatorId not in item.annotations:
                # Signal item that this annotator is holding it.
                item.holding[annotatorId] = {
                    "time": datetime.now(tz.tzlocal())
                }

                # Store that this annotator is holding the item.
                self.mapAnnotatorItem[annotatorId] = item

                # Remove this item copy form the list.
                del self.partiallyAnnotatedItems[i]

                return itemId, item

        # Check if there is some unannotated item available. Otherwise, wait.
        while len(self.unannotatedItems) == 0 and self.running:
            self.__condition.wait()

        if self.running:
            # Get one unannotated item.
            (itemId, item) = AnnotatedItem(self.unannotatedItems.pop(0))
            if len(self.unannotatedItems) < self.numUnannotatedItems / 2:
                # Notify producer thread if the list length is less than half of the required length.
                self.__condition.notifyAll()

            # Signal item that this annotator is holding it.
            item.holding[annotatorId] = {
                "time": datetime.now(tz.tzlocal())
            }

            # Store that this annotator is holding the item.
            self.mapAnnotatorItem[annotatorId] = (itemId, item)

            # Insert copies of the item in the partially annotated list, so next annotators can get this item.
            self.partiallyAnnotatedItems += [(itemId, item)] * (self.numAnnotationsPerItem - 1)

            return itemId, item.doc

        # Something odd occurred.
        self.logger.error("No item could be retrieved for annotator %s" % annotatorId)
        return None
