# coding=utf-8
from datetime import datetime
from threading import Thread, Condition

from dateutil import tz
from elasticsearch.helpers import scan

from annotated_item import AnnotatedItem


class AnnotationManager(Thread):
    """
    The annotation manager is responsible to retrieve items to be annotated to coming annotators. It takes care
    of how many annotations were attached to each item and which item is held by each annotator. In order to manage all
    this, this class employs two lists of items: unannotated items and partially annotated items. The list of
    unannotated items store available items which have not been annotated by any annotators. Any of these items can
    be returned to an annotator that has no partially annotated item available.

    The list of partially labeled items store items that have been annotated by some annotator but still lack some
    annotation(s) in order to fulfill the required number of annotations for each item. Thus, when some annotator asks
    for a new item, these items (partially annotated) are the first considered. If there is some item not annotated
    by the given annotator, then the manager returns such item. Only when the given annotator has already annotated all
    partially annotated items, the manager will get an unannotated item. This item will then be allocated to the given
    annotator, removed from the list of unannotated items and added to the list of partially annotated items.

    In fact, since each item (potentially) needs to be annotated by more than one annotator, the manager adds a copy
    of an unannotated item for each annotation required for this item.

    There other two data structures used by the manager to control which annotator is holding which item. When an
    annotator gets a item to be annotated, the manager does not know whether the annotator will really annotate the
    item or will just leave the system, for instance. So, the manager stays on the safe side and considers that the
    annotator will eventually annotated the given item. The manger then includes an entry in the heldItems dictionary
    in which the key is the annotator id and the value is the associated item.

    The annotation manager object is a singleton within the application, i.e., there is only one object that is
    shared by all requests/users.
    """

    def __init__(self, name, esClient, index, annotationType, annotationName, numAnnotationsPerItem, logger):
        """
        Create a new annotation manager object and spawn a new thread to produce new annotation items.

        :param name: friendly, but unique, name for this manager.
        :param esClient: Elasticsearch client.
        :param index: index in ES to be used.
        :param annotationType: document type for annotations (tweets).
        :param annotationName: task name which identifies the annotation task (all items have this name).
        :param numAnnotationsPerItem: number of annotations to be collected for each item.
        :param logger: logger object.
        """
        super(AnnotationManager, self).__init__(name="AnnotationManager-%s" % name)

        self.name = name
        self.es = esClient
        self.index = index
        self.annotationType = annotationType
        self.annotationName = annotationName
        self.numAnnotationsPerItem = numAnnotationsPerItem
        self.logger = logger

        # Condition variable used to coordinate the producer (the manager thread) and the consumers (request threads).
        self.__condition = Condition()

        # Size of the queue of unannotated items. The producer thread will keep this number of items always available.
        self.numUnannotatedItems = 10

        # List of unannotated items retrieved and, thus, available to be annotated by any annotator.
        self.unannotatedItems = []

        # List of items which have been annotated by some annotator but has not yet been annotated by the required
        # number of annotators (self.numAnnotationsPerItem).
        self.partiallyAnnotatedItems = []

        # This dictionary stores, for each annotator, the item it is holding (the one returned by self.getItem()).
        self.heldItems = {}

        # Number of items already returned within the query for unannotated items since the AnnotationManager started.
        # This index allows to query only new items and it is needed.
        self.searchFrom = 0

        # Flag to indicate whether the AnnotationManager thread is running or not.
        self.running = False

        # Spawn the AnnotationManager thread. This thread takes care of filling the list of unannotated items.
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
                if lenUn < self.numUnannotatedItems / 2:
                    self.__fillUnannotatedItems()

                    if len(self.unannotatedItems) == 0:
                        self.running = False
                        self.__condition.notifyAll()
                        break

                    # Notify consumers if the list was empty.
                    if lenUn == 0 and len(self.unannotatedItems) > 0:
                        self.__condition.notifyAll()

                self.__condition.wait()

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
            if annotatorId in self.heldItems:
                item = self.heldItems[annotatorId]
                # Update the obtained time for this item.
                item.holdingAnnotators[annotatorId]["time"] = datetime.now(tz.tzlocal())
                return item

            return self.__nextItem(annotatorId)

    def annotate(self, annotatorId, itemId, annotation):
        """
        Save the given annotation and return a new associated item.

        :param annotatorId:
        :param itemId:
        :param annotation:
        :return: a new associated item for the given annotator.
        """
        with self.__condition:
            # Check holding dictionaries.
            if not self.__checkHeldItem(annotatorId, itemId):
                return self.__nextItem(annotatorId)

            # Item held by the given annotator.
            item = self.heldItems[annotatorId]

            # Append the given annotation.
            item.annotations[annotatorId] = {
                "annotation": annotation,
                "time": datetime.now(tz.tzlocal())
            }

            # Increment valid annotations count.
            item.numValidAnnotations += 1

            # Update Elasticsearch.
            self.es.update(index=self.index, doc_type=self.annotationType, id=item.id,
                           body={"doc": item.getSourceToUpdate()})

            # Remove annotator from the item's holding dictionary.
            del item.holdingAnnotators[annotatorId]

            # Remove item from the held-items dictionary.
            del self.heldItems[annotatorId]

            # Return a new item.
            return self.__nextItem(annotatorId)

    def invalidate(self, annotatorId, itemId, cause):
        """
        Invalidate the given item. This item will never be returned to annotation of any annotator.

        :param annotatorId:
        :param itemId:
        :param cause:
        :return:
        """
        with self.__condition:
            # Check holding dictionaries.
            if not self.__checkHeldItem(annotatorId, itemId):
                return self.__nextItem(annotatorId)

            item = self.heldItems[annotatorId]
            item.invalid = {
                "annotatorId": annotatorId,
                "cause": cause,
                "time": datetime.now(tz.tzlocal())
            }

            # Update Elasticsearch.
            self.es.update(index=self.index, doc_type=self.annotationType, id=item.id,
                           body={"doc": item.getSourceToUpdate()})

            # Remove annotator from the item's holding dictionary.
            del item.holdingAnnotators[annotatorId]

            # Unlink item and annotator.
            del self.heldItems[annotatorId]

            # Remove other occurrences of the invalidated item from the list of partially annotated items.
            self.partiallyAnnotatedItems = [i for i in self.partiallyAnnotatedItems if i != item]

            # Return next item.
            return self.__nextItem(annotatorId)

    def skip(self, annotatorId, itemId):
        """
        Mark the given item as skipped for the given annotator. This item will never be retrieved for this annotator.

        :param annotatorId:
        :param itemId:
        :return:
        """
        with self.__condition:
            # Check holding dictionaries.
            if not self.__checkHeldItem(annotatorId, itemId):
                return self.__nextItem(annotatorId)

            item = self.heldItems[annotatorId]

            # Append the given annotation.
            item.annotations[annotatorId] = {
                "annotation": "skip",
                "time": datetime.now(tz.tzlocal())
            }

            # Update Elasticsearch.
            self.es.update(index=self.index, doc_type=self.annotationType, id=item.id,
                           body={"doc": item.getSourceToUpdate()})

            # Remove annotator from the item's holding dictionary.
            del item.holdingAnnotators[annotatorId]

            # Unlink item and annotator.
            del self.heldItems[annotatorId]

            # Include back the skipped item in the list of partially annotated items.
            self.partiallyAnnotatedItems.append(item)

            # Return the next item associated to the given annotator.
            return self.__nextItem(annotatorId)

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
            item = self.partiallyAnnotatedItems[i]
            if annotatorId not in item.annotations:
                # Signal item that this annotator is holding it.
                item.holdingAnnotators[annotatorId] = {
                    "time": datetime.now(tz.tzlocal())
                }

                # Store that this annotator is holding the item.
                self.heldItems[annotatorId] = item

                # Remove this item copy form the list.
                del self.partiallyAnnotatedItems[i]

                return item

        # Check if there is some unannotated item available. Otherwise, wait.
        while len(self.unannotatedItems) == 0 and self.running:
            self.__condition.wait()

        if self.running:
            # Get one unannotated item.
            item = self.unannotatedItems.pop(0)
            if len(self.unannotatedItems) < self.numUnannotatedItems / 2:
                # Notify producer thread if the list length is less than half of the required length.
                self.__condition.notifyAll()

            # Signal item that this annotator is holding it.
            item.holdingAnnotators[annotatorId] = {
                "time": datetime.now(tz.tzlocal())
            }

            # Store that this annotator is holding the item.
            self.heldItems[annotatorId] = item

            # Insert copies of the item in the partially annotated list, so next annotators can get this item.
            self.partiallyAnnotatedItems += [item] * (self.numAnnotationsPerItem - 1)

            return item

        # Something odd occurred.
        self.logger.error("No item could be retrieved for annotator %s" % annotatorId)
        return None

    def __fillPartiallyAnnotatedItems(self):
        """
        Fill the self.partiallyAnnotatedItems list with all items from Elasticsearch that includes some annotation but
        not the required number (self.numAnnotationsPerItem).
        """
        # Query: numValidAnnotations < self.numAnnotationsPerItem and annotations != None and invalid == None
        _scan = scan(self.es, index=self.index, doc_type=self.annotationType, query={
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "name": self.annotationName
                            }
                        },
                        {
                            "range": {
                                "numValidAnnotations": {
                                    "lt": self.numAnnotationsPerItem
                                }
                            }
                        },
                        {
                            "exists": {
                                "field": "annotations"
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

        for res in _scan:
            item = AnnotatedItem(res["_id"], res["_source"])
            # Include one copy of this item for each missing annotation.
            self.partiallyAnnotatedItems += [item] * (self.numAnnotationsPerItem - item.numValidAnnotations)

    def __fillUnannotatedItems(self):
        """
        Fill the list of unannotated items with the next items.
        These items have not been annotated by any annotator.
        """
        # Number of unannotated items to retrieve in order to fill the list.
        n = self.numUnannotatedItems - len(self.unannotatedItems)

        # Search n new items from the previous point (self.searchFrom).
        # annotations == None and invalid == None
        res = self.es.search(index=self.index, doc_type=self.annotationType, body={
            "from": self.searchFrom,
            "size": n,
            "sort": "docId",
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        "name": self.annotationName
                                    }
                                }
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
                    # "random_score": {
                    #     "seed": 13
                    # },
                    # "boost_mode": "replace"
                }
            }
        })

        hits = res["hits"]["hits"]

        # Update from index.
        self.searchFrom += len(hits)

        # Append the retrieved items to the list.
        for hit in hits:
            item = AnnotatedItem(hit["_id"], hit["_source"])
            self.unannotatedItems.append(item)

        if len(self.unannotatedItems) == 0:
            self.logger.error("Unavailable items to annotate")

    def __checkHeldItem(self, annotatorId, itemId):
        """
        Check if the given item is held by the given annotator. That means to verify if the annotator is in the list of
        holding annotators of the item and if the item is associated with the annotator in the self.heldItems
        dictionary.

        :param annotatorId:
        :param itemId:
        :return:
        """
        # Check if the annotator is holding some item.
        if annotatorId not in self.heldItems:
            self.logger.error(
                "Annotator %s tried to annotate item %s but s/he was not holding this item. Getting a new one." % (
                    annotatorId, itemId))
            return False

        # Check if the annotator is holding the given item.
        item = self.heldItems[annotatorId]
        if annotatorId not in item.holdingAnnotators:
            self.logger.error(
                "Annotator %s tried to annotate item %s but s/he was not holding this item. Getting a new one." % (
                    annotatorId, itemId))
            del self.heldItems[annotatorId]
            return False

        return True
