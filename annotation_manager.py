# coding=utf-8
from random import randrange


class AnnotationManager:
    """
    The annotation manager is responsible to keep a list of available tweets to be annotated. It also manages
    which tweet is associated to each user of the system, and saves the given annotations to ES.

    The annotation manager object is a singleton within the application, i.e., there is only one object that is
    shared by all requests/users.
    """

    def __init__(self, _es):
        """
        Create the annotation object with the given ES client.

        :param _es: the ES client to be used by this manager.
        """
        self.es = _es
        _tweets = _es.search(index="ctrls_001", doc_type="twitter", body={
            "size": 10,
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        "start": "2017-02-20T16:33:25.093458-04:00"
                                    }
                                },
                                {
                                    "range": {
                                        "tweet.created_at": {
                                            "gte": "2017-02-20",
                                            "lt": "2017-03-21"
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "random_score": {},
                    "boost_mode": "replace"
                }
            }
        })["hits"]["hits"]
        self.tweets = [t["_source"]["tweet"] for t in _tweets]
        self.mapUserTweet = {}

    def getCurrentTweet(self, userId):
        """
        Return the current tweet associated to the given user (userId).

        :param userId:
        :return:
        """
        tweet = self.mapUserTweet.get(userId)
        if tweet is None:
            tweet = self.__nextTweet(userId)
        return tweet

    def __nextTweet(self, userId):
        """
        Choose a new tweet to be annotated by the given user from the list of available tweets.

        :param userId:
        :return: the new associated tweet.
        """
        n = len(self.tweets)
        i = randrange(n)
        tweet = self.tweets[i]
        del self.tweets[i]
        self.mapUserTweet[userId] = tweet
        return tweet

    def skipTweet(self, userId):
        """
        Skip the current tweet associated with the given user ID and associate a new tweet to this user.

        :param userId:
        :return: the new associated tweet.
        """
        return self.__nextTweet(userId)

    def annotate(self, userId, tweetId, annotation):
        """
        Save the given annotation to ES and return a new associated tweet.
        If the given annotation is None, it is equivalent to skipTweet(userId).

        :param userId:
        :param tweetId:
        :param annotation: valid values are 'yes', 'no' and None.
        :return: a new associated tweet.
        """
        # Retrieve user annotations.
        _doc = self.es.get(index='test', doc_type='anotadores', id=userId)['_source']

        # Append the given annotation (if any).
        if annotation in ("yes", "no"):
            # Append the given annotation.
            annotations = _doc.setdefault('annotations', [])
            annotations.append({'tweet_id_str': tweetId, 'annotation': annotation})
            # Update Elasticsearch.
            self.es.update(index="test", doc_type="anotadores", id=userId, body={"doc": _doc})

        # Return next available tweet.
        return self.__nextTweet(userId)
