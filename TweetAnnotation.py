#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from random import randrange
import flask
from flask import Flask, render_template, request, session, redirect, flash, current_app
from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict
from werkzeug.local import LocalProxy
from elasticsearch import Elasticsearch
import requests
import time
from uuid import uuid4

app = Flask(__name__)
app.secret_key = '\x1c\xfb|o\xcc\r\x96\xc4\xe4\xfe\xaf\xb9\x16b\x96n0+{Nd|+\xd4'


def getElasticSearchClient():
    """
    Elasticsearch client is bounded to the request (flask.g).
    This function returns the bounded client or create a new one.

    :return: the Elasticsearch client bounded to this request.
    """
    _es = getattr(flask.g, 'es', None)
    if _es is None:
        _es = flask.g.es = Elasticsearch(['http://localhost:9200'])
    return _es


def getAnnotationManager():
    """
    The annotation manager object manages which tweets are available for annotation and return one tweet for each
    user that logs in the system. It is also responsible for saving annotations to ES.

    :return: the annotation manager object.
    """
    with app.app_context():
        _annManager = getattr(current_app, 'annManager', None)
        if _annManager is None:
            _annManager = current_app.annManager = AnnotationManager(Elasticsearch(['http://localhost:9200']))
        return _annManager


# Proxy variable to the Elasticsearch client.
es = LocalProxy(getElasticSearchClient)

# Proxy variable to the annotation manager object.
annManager = LocalProxy(getAnnotationManager)


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
        _doc = es.get(index='test', doc_type='anotadores', id=userId)['_source']

        # Append the given annotation (if any).
        if annotation in ("yes", "no"):
            # Append the given annotation.
            annotations = _doc.setdefault('annotations', [])
            annotations.append({'tweet_id_str': tweetId, 'annotation': annotation})
            # Update Elasticsearch.
            es.update(index="test", doc_type="anotadores", id=userId, body={"doc": _doc})

        # Return next available tweet.
        return self.__nextTweet(userId)


class ElasticsearchSession(CallbackDict, SessionMixin):
    def __init__(self, userId):
        super(ElasticsearchSession, self).__init__()
        self.userId = userId


class ElasticSearchSessionInterface(SessionInterface):
    def open_session(self, app, request):
        userId = request.cookies.get(app.session_cookie_name)
        if userId is None:
            userId = str(uuid4())
            es.index(index="test", doc_type="anotadores", id=userId, body={})
        return ElasticsearchSession(userId)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if session is None:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            return

        # TODO: Jonatas, você tem certeza que é necessário apagar o cookie antes de setá-lo?
        response.delete_cookie(app.session_cookie_name, domain=domain)
        expires = time.time() + 3650 * 24 * 3600
        response.set_cookie(app.session_cookie_name, session.userId,
                            expires=time.strftime("%a, %d-%b-%Y %T GMT", time.gmtime(expires)),
                            httponly=True, domain=domain)


@app.route('/')
def index():
    """
    Render the annotation page using the current tweet for the logged user.
    :return:
    """
    # Get the oEmbed HTML for the current tweet of the logged user.
    tweet = annManager.getCurrentTweet(session.userId)
    tweetUrl = 'https://twitter.com/%s/status/%s' % (tweet["user"]["screen_name"], tweet["id_str"])
    oEmbedUrl = 'https://publish.twitter.com/oembed?hide_thread=t&url=%s' % tweetUrl
    oEmbedResp = requests.get(oEmbedUrl)

    if oEmbedResp.status_code != 200:
        # Não retornou com sucesso (por alguma razão que desconheço).
        annManager.skipTweet(session.userId)
        return redirect('/')

    # Load the returned tweet JSON.
    tweetJson = json.loads(oEmbedResp.content)

    if 'html' not in tweetJson:
        # A API do Twitter retornou algum erro. Em geral, o tweet foi removido ou não é mais público.
        annManager.skipTweet(session.userId)
        return redirect('/')

    # Get the HTML content.
    tweetHtml = tweetJson['html']

    # Render the annotation page.
    return render_template('tweet_annotation.html', userId=session.userId, tweetId=tweet['id_str'],
                           tweet=tweetHtml, context=u"à série Supernatural")


@app.route('/annotate', methods=['GET', 'POST'])
def annotateTweet():
    """
    Process the annotation form request.
    :return:
    """
    if request.method == 'GET':
        return redirect('/')

    # Check if the logged user is making the request.
    userId = session.userId
    if request.form.get('userId') != userId:
        flash(u'Usuário com IDs inconsistentes!')
        app.logger.error(u'Usuário com IDs inconsistentes (%s != %s) quando anotando o tweet %s' % (userId,
                                                                                                    request.form.get(
                                                                                                        'userId'),
                                                                                                    request.form.get(
                                                                                                        'tweetId')))
        return redirect('/')

    # Check if the given tweet annotation is related to the current tweet for the logged user.
    # This can fail when the user refresh a previous submitted form.
    tweetId = annManager.getCurrentTweet(userId)['id_str']
    if request.form.get('tweetId') != tweetId:
        flash(u'Anotação de tweet com ID inconsistente!')
        app.logger.error(u'Anotação de tweet com ID inconsistente (%s != %s) / userId: %s' % (tweetId,
                                                                                              request.form.get(
                                                                                                  'tweetId'),
                                                                                              userId))
        return redirect('/')

    # Get the provided annotation.
    annotation = request.form.get("answer")

    # Save it to ES.
    annManager.annotate(userId, tweetId, annotation)

    # Move to the next tweet.
    flash('Tweet analisado com sucesso!')
    app.logger.info(u'Usuário %s anotou o tweet %s como %s' % (userId, tweetId, annotation))
    return redirect('/')


if __name__ == '__main__':
    with app.app_context():
        current_app.annManager = AnnotationManager(Elasticsearch(['http://localhost:9200']))
    app.session_interface = ElasticSearchSessionInterface()
    app.run(host='127.0.0.1')
