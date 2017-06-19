#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests
from codecs import open

from elasticsearch import Elasticsearch
from flask import Flask, render_template, request, session, redirect, flash, current_app, abort

from annotation_manager import AnnotationManager
from session_manager import ElasticsearchSessionInterface

app = Flask(__name__)

# Load keys for each context.
with open('context_config.json') as f:
    contextConfig = json.load(f)

# Load app secret key from file.
app.secret_key = None
with open('app_secret_key', 'rt', encoding='utf8') as f:
    app.secret_key = f.read()


def getElasticsearchClient():
    """
    Elasticsearch client is bounded to the request (flask.g).
    This function returns the bounded client or create a new one.

    :return: the Elasticsearch client bounded to this request.
    """
    with app.app_context():
        _es = getattr(current_app, 'esClient', None)
        if _es is None:
            _es = current_app.esClient = Elasticsearch(['http://localhost:9200'])
        return _es


def getAnnotationManager(key):
    """
    The annotation manager object manages which tweets are available for annotation and return one tweet for each
    user that logs in the system. It is also responsible for saving annotations to ES.

    This object is bounded to the web app (app context). This means that there is only one object per application
    and it is responsible for managing the lists of tweets for all users. Thus, it needs to deal with race conditions,
    caused by the request threads.

    :param key: key to the current context (this should be part of the request URL).

    :return: the annotation manager object.
    """
    with app.app_context():
        _contextConfig = getattr(current_app, 'contextConfig', None)
        if _contextConfig is None:
            _contextConfig = current_app.contextConfig = contextConfig

        if key not in _contextConfig:
            return None

        _context = _contextConfig[key]
        _annManager = _context.get("annotationManager")
        if _annManager is None:
            _annManager = AnnotationManager(name=_context["name"], esClient=getElasticsearchClient(),
                                            index="ctrls_annotation_no_retweet", annotationType="relevance",
                                            annotationName=_context["name"], numAnnotationsPerItem=2, logger=app.logger)
            _context["annotationManager"] = _annManager

        return _annManager


@app.route('/<key>/login', methods=['GET', 'POST'])
def login(key):
    if getAnnotationManager(key) is None:
        abort(404)

    if request.method == 'GET':
        if session.userEmail is not None:
            # User is logged already.
            return redirect('/%s' % key)
        return render_template("login_page.html", userID=session.userId, key=key)
    else:  # request.method == 'POST'
        email = request.form.get('email')
        session.login(email)
        return redirect('/%s' % key)


@app.route('/<key>/logout', methods=['GET', 'POST'])
def logout(key):
    if getAnnotationManager(key) is None:
        abort(404)

    session.logout()
    return redirect('/%s' % key)


@app.route('/<key>/', methods=['GET', 'POST'])
def index(key):
    """
    Render the annotation page using the current tweet for the logged user.
    :return:
    """

    annManager = getAnnotationManager(key)
    if annManager is None:
        abort(404)

    # Checks if the user has an e-mail attached to it
    if session.userEmail is None:
        return redirect('/%s/login' % key)

    if request.form.get('submit') == "Logout":
        session.userEmail = None
        return redirect('/%s/login' % key)

    # Get current item for the logged user.
    item = annManager.getItem(session.userId)
    if item is None:
        app.logger.error("No item to be annotated!")
        return render_template('tweet_annotation.html', userId=session.userId, email=session.userEmail, key=key,
                               message="Todos os tweets foram anotados. Obrigado!")

    tweet = item.doc["tweet"]

    tweetUrl = 'https://twitter.com/%s/status/%s' % (tweet["user"]["screen_name"], tweet["id_str"])
    oEmbedUrl = 'https://publish.twitter.com/oembed?hide_thread=t&url=%s' % tweetUrl
    oEmbedResp = requests.get(oEmbedUrl)

    if oEmbedResp.status_code != 200:
        # Não retornou com sucesso (por alguma razão que desconheço).
        annManager.invalidate(session.userId, item.id, "Unexpected status code %d" % oEmbedResp.status_code)
        return redirect('/%s' % key)

    # Load the returned tweet JSON.
    tweetJson = json.loads(oEmbedResp.content)

    if 'html' not in tweetJson:
        # A API do Twitter retornou algum erro. Em geral, o tweet foi removido ou não é mais público.
        annManager.invalidate(session.userId, tweet.id, "Tweet nulo!")
        return redirect('/%s' % key)

    # Get the HTML content.
    tweetHtml = tweetJson['html']

    # Render the annotation page.
    return render_template('tweet_annotation.html', userId=session.userId, tweetId=item.id, key=key,
                           tweet=tweetHtml, context=item.contextDescription, email=session.userEmail)


@app.route('/<key>/annotate', methods=['GET', 'POST'])
def annotateTweet(key):
    """
    Process the annotation form request.
    :return:
    """
    annManager = getAnnotationManager(key)
    if annManager is None:
        abort(404)

    if request.method == 'GET':
        return redirect('/%s' % key)

    # Check if the logged user is making the request.
    userId = session.userId
    if request.form.get('userId') != userId:
        flash(u'Usuário com IDs inconsistentes!')
        app.logger.error(u'Usuário com IDs inconsistentes (%s != %s) quando anotando o tweet %s' % (userId,
                                                                                                    request.form.get(
                                                                                                        'userId'),
                                                                                                    request.form.get(
                                                                                                        'tweetId')))
        return redirect('/%s' % key)

    # Check if the given tweet annotation is related to the current tweet for the logged user.
    # This can fail when the user refresh a previous submitted form.
    # Get current item for the logged user.
    item = annManager.getItem(session.userId)
    if request.form.get('tweetId') != item.id:
        flash(u'Anotação de tweet com ID inconsistente!')
        app.logger.error(u'Anotação de tweet com ID inconsistente (%s != %s) / userId: %s' % (item.id,
                                                                                              request.form.get(
                                                                                                  'tweetId'),
                                                                                              userId))
        return redirect('/%s' % key)

    # Get the provided annotation.
    annotation = request.form.get("submit")

    if annotation in ("Sim", "Nao"):
        # Save it to ES.
        annManager.annotate(userId, item.id, annotation)
    elif annotation == "Nao Sei":
        # Skip this item.
        annManager.skip(userId, item.id)
    else:
        app.logger.error("Unknown annotation %s" % annotation)
        return redirect('/%s' % key)

    # Move to the next tweet.
    flash('Tweet analisado com sucesso!')
    app.logger.info(u'Usuário %s anotou o item %s como %s' % (userId, item.id, annotation))
    return redirect('/%s' % key)


if __name__ == '__main__':
    app.session_interface = ElasticsearchSessionInterface(getElasticsearchClient(), index='ctrls', docType='annotator')
    app.run(host='0.0.0.0')
