#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests

import flask
from elasticsearch import Elasticsearch
from flask import Flask, render_template, request, session, redirect, flash, current_app
from werkzeug.local import LocalProxy
from codecs import open

from annotation_manager import AnnotationManager
from session_manager import ElasticSearchSessionInterface

app = Flask(__name__)
# app.secret_key = '\x1c\xfb|o\xcc\r\x96\xc4\xe4\xfe\xaf\xb9\x16b\x96n0+{Nd|+\xd4'
app.secret_key = None
with open('app_secret_key', 'rt', 'utf8') as f:
    app.secret_key = f.read()


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

    This object is bounded to the web app (app context). This means that there is only one object per application
    and it is responsible for managing the lists of tweets for all users. Thus, it needs to deal with race conditions,
    caused by the request threads.

    :return: the annotation manager object.
    """
    with app.app_context():
        _annManager = getattr(current_app, 'annManager', None)
        if _annManager is None:
            _annManager = AnnotationManager(name="teste", esClient=Elasticsearch(['http://localhost:9200']),
                                            index="test_annotation_index", annotationType="test_annotation",
                                            annotationName="teste", numAnnotationsPerItem=2, logger=app.logger)
            current_app.annManager = _annManager
        return _annManager


# Proxy variable to the Elasticsearch client.
es = LocalProxy(getElasticSearchClient)

# Proxy variable to the annotation manager object.
annManager = LocalProxy(getAnnotationManager)


@app.route('/login', methods=['GET', 'POST'])
def emailLogin():
    email = request.form.get('email')
    email_dict = {'email': email}

    # Check if the logged user is making the request.
    userId = session.userId
    if request.form.get('userId') != userId:
        flash(u'Usuário com IDs inconsistentes!')
        app.logger.error(
            u'Usuário com IDs inconsistentes (%s != %s) ao tentar logar!' % (userId, request.form.get('userId')))
        return redirect('/')

    es.update(index="test", doc_type="anotadores", id=userId, body={'doc': email_dict})
    return redirect('/')


@app.route('/')
def index():
    print session.userId
    """
    Render the annotation page using the current tweet for the logged user.
    :return:
    """

    # Checks if the user has an e-mail attached to it
    if session.email is None:
        return render_template("login_page.html", userId=session.userId)

    # Get the oEmbed HTML for the current tweet of the logged user.
    item = annManager.getItem(session.userId)
    if item is None:
        app.logger.error("No item to be annotated!")
        return render_template('tweet_annotation.html', userId=session.userId,
                               message="Todos os tweets foram anotados. Obrigado!")

    # tweet = item.doc["tweet"]
    # tweetUrl = 'https://twitter.com/%s/status/%s' % (tweet["user"]["screen_name"], tweet["id_str"])
    # oEmbedUrl = 'https://publish.twitter.com/oembed?hide_thread=t&url=%s' % tweetUrl
    # oEmbedResp = requests.get(oEmbedUrl)
    #
    # if oEmbedResp.status_code != 200:
    #     # Não retornou com sucesso (por alguma razão que desconheço).
    #     # Invalida tweet para sempre.
    #     annManager.annotate(session.userId, item.id,
    #                         "Code: %d / Reason: %s" % (oEmbedResp.reason, oEmbedResp.status_code),
    #                         invalidate=True)
    #     return redirect('/')
    #
    # # Load the returned tweet JSON.
    # tweetJson = json.loads(oEmbedResp.content)
    #
    # if 'html' not in tweetJson:
    #     # A API do Twitter retornou algum erro. Em geral, o tweet foi removido ou não é mais público.
    #     annManager.annotate(session.userId, item.id, "Twitter API returned an error!", invalidate=True)
    #     app.logger.error(str(tweetJson))
    #     return redirect('/')
    #
    # # Get the HTML content.
    # tweetHtml = tweetJson['html']

    if item.docId % 5 == 0:
        annManager.invalidate(session.userId, item.id, "Erro simulado")
        return redirect('/')

    tweetHtml = "<h3>%s</h3>" % item.doc

    # Render the annotation page.
    return render_template('tweet_annotation.html', userId=session.userId, tweetId=item.id,
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
    item = annManager.getItem(userId)
    itemId = item.id
    if request.form.get('tweetId') != itemId:
        flash(u'Anotação de tweet com ID inconsistente!')
        app.logger.error(u'Anotação de tweet com ID inconsistente (%s != %s) / userId: %s' % (itemId,
                                                                                              request.form.get(
                                                                                                  'tweetId'),
                                                                                              userId))
        return redirect('/')

    # Get the provided annotation.
    annotation = request.form.get("answer")

    if annotation is None:
        annManager.skip(userId, itemId)
    else:
        annManager.annotate(userId, itemId, annotation)

    # Move to the next tweet.
    flash('Tweet analisado com sucesso!')
    app.logger.info(u'Usuário %s anotou o tweet %s como %s' % (userId, itemId, annotation))
    return redirect('/')


if __name__ == '__main__':
    # Call this method here in order to create the singleton before starting the app.
    getAnnotationManager()

    app.session_interface = ElasticSearchSessionInterface(es)
    app.run(host='127.0.0.1')
