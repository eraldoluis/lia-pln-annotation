class AnnotatedItem:
    """
    Utility class to simplify access to holding dictionary and annotation for an item.
    """

    def __init__(self, id, source):
        self.id = id
        self.doc = source["doc"]
        self.docId = source["docId"]

        self.numValidAnnotations = source.get("numValidAnnotations", 0)

        self.annotations = {}
        if "annotations" in source:
            for ann in source["annotations"]:
                self.annotations[ann["annotatorId"]] = {
                    "annotation": ann["annotation"],
                    "time": ann["time"]
                }

        # Holding annotators dictionary (key is annotatorId).
        self.holdingAnnotators = {}

        # Possible invalidation caused by some annotator.
        self.invalid = source.get("invalid")

    def getSourceToUpdate(self):
        """
        Return a source to update the annotation-related field of this item.
        :return:
        """
        annotations = []
        for (annotatorId, annotation) in self.annotations.iteritems():
            annotations.append({
                "annotatorId": annotatorId,
                "annotation": annotation["annotation"],
                "time": annotation["time"]
            })

        source = {
            "numValidAnnotations": self.numValidAnnotations,
            "annotations": annotations
        }

        if self.invalid is not None:
            source["invalid"] = self.invalid

        return source
