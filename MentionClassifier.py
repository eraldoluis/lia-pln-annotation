#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from random import randint
from flask import Flask, url_for, render_template, request, session, redirect
from flask import flash
from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict
from elasticsearch import Elasticsearch
import requests
import time
from uuid import uuid4

app = Flask(__name__)


class ElasticsearchSession(CallbackDict, SessionMixin):
    def __init__(self, initial=None, sid=None, client=Elasticsearch(['http://localhost:9200'])):
        self.storedSession = initial
        self.sid = sid
        self.client = client
        self.modified = False
        # self.mentionIndex = randint(0, 4999)
        if self.storedSession is None:
            self.storedSession = {
                "annotations": [],
                "mentionIndex": randint(0, 4999)
            }
            self.client.index(index="test", doc_type="anotadores", id=sid, body=self.storedSession)

        # Lista de tweets a serem analisados.
        self.results = self.client.search(index="ctrls_001", doc_type="twitter", body={
            "size": 5000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": "tweet.text:sam OR tweet.text:dean",
                                "analyze_wildcard": "true"
                            }
                        },
                        {
                            "range": {
                                "tweet.created_at": {
                                    "from": "2017-03-01T13:00-04:00",
                                    "to": "2017-03-20T14:00-04:00"
                                }
                            }
                        }
                    ]
                }
            }
        })["hits"]["hits"]

    def getNextTweet(self):
        mentionIndex = randint(0, 4999)
        self.storedSession["mentionIndex"] = mentionIndex
        self.client.update(index="test", doc_type="anotadores", id=session.sid,
                           body={"doc": {"mentionIndex": mentionIndex}})


class ElasticSearchSessionInterface(SessionInterface):
    def __init__(self, index='test'):
        self.client = Elasticsearch(['http://localhost:9200'])

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if sid:
            try:
                stored_session = self.client.get(index='test', doc_type='anotadores', id=sid)
                return ElasticsearchSession(initial=stored_session['_source'], sid=stored_session['_id'])
            except:
                print "Usuario nao encontrado\n"
        sid = str(uuid4())
        return ElasticsearchSession(sid=sid)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if session is None:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            return

        # TODO: Jonatas, você tem certeza que é necessário apagar o cookie antes de setá-lo?
        response.delete_cookie(app.session_cookie_name, domain=domain)
        expires = time.time() + 3650 * 24 * 3600
        response.set_cookie(app.session_cookie_name, session.sid,
                            expires=time.strftime("%a, %d-%b-%Y %T GMT", time.gmtime(expires)),
                            httponly=True, domain=domain)


@app.route('/')
def MentionGetter():
    # Get the current tweet.
    mentionIndex = session.storedSession["mentionIndex"]
    tweet = session.results[mentionIndex]["_source"]["tweet"]
    tweetUrl = 'https://twitter.com/%s/status/%s' % (tweet["user"]["screen_name"], tweet["id_str"])
    oEmbedUrl = 'https://publish.twitter.com/oembed?url=%s' % tweetUrl
    tweetJson = json.loads(requests.get(oEmbedUrl).content)
    if 'html' not in tweetJson:
        # A API do Twitter retornou algum erro. Em geral, o tweet foi removido ou não é mais público.
        session.getNextTweet()
        return redirect('/')
    tweetHtml = tweetJson['html']
    return render_template('MentionDisplay.html', tweet=tweetHtml, context=u"à série Supernatural")


@app.route('/MentionClassificated', methods=['GET', 'POST'])
def MentionClassifier():
    if request.method == 'GET':
        return redirect('/')

    answer = request.form.get("answer")
    if answer not in ("yes", "no"):
        session.getNextTweet()
        return redirect('/')

    annotations = session.storedSession["annotations"]
    mentionIndex = session.storedSession["mentionIndex"]
    annotations.append({"tweet_id_str": session.results[mentionIndex]["_source"]["tweet"]["id_str"],
                        "annotation": answer})
    session.client.update(index="test", doc_type="anotadores", id=session.sid,
                          body={"doc": {"annotations": annotations}})
    session.getNextTweet()
    flash('Tweet analisado com sucesso!')
    return redirect('/')


if __name__ == '__main__':
    app.session_interface = ElasticSearchSessionInterface()
    app.run(host='127.0.0.1')
