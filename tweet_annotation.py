#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import requests

import flask
from elasticsearch import Elasticsearch
from flask import Flask, render_template, request, session, redirect, flash, current_app
from werkzeug.local import LocalProxy

from annotation_manager import AnnotationManager
from session_manager import ElasticSearchSessionInterface

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

    This object is bounded to the web app (app context). This means that there is only one object per application
    and it is responsible for managing the lists of tweets for all users. Thus, it needs to deal with race conditions,
    caused by the request threads.

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

@app.route('/login',methods=['GET', 'POST'])
def emailLogin():
    email = request.form.get('email')
    try:
        r = es.search(index="test",doc_type="anotadores",body={
            "query": {
                "match": {
                    "email": email
                }
            }
        })
        session.userId = r['hits']['hits']['_score']['_id']
        response.delete_cookie(app.session_cookie_name, domain=domain)
        response.set_cookie(app.session_cookie_name, session.userId,
                            expires=time.strftime("%a, %d-%b-%Y %T GMT", time.gmtime(expires)),
                            httponly=True, domain=domain)
    except:
        pass
    email_dict = {'email': email}
    print email_dict
    userID = request.form.get('userID')
    print userID
    es.update(index="test", doc_type="anotadores", id=userID, body={'doc':email_dict})
    return redirect('/')



@app.route('/',methods=['GET','POST'])
def index():
    """
    Render the annotation page using the current tweet for the logged user.
    :return:
    """
    if(request.form.get('submit') == "Logout"):
        app.session_interface.email = None
        return render_template("login_page.html", userID=session.userId)
    # Checks if the user has an e-mail attached to it
    if(app.session_interface.email is None):
        return render_template("login_page.html",userID=session.userId)



    # Get the oEmbed HTML for the current tweet of the logged user.
    if request.args.get('pular') is None:
        tweet = annManager.getCurrentTweet(session.userId)
    else:
        annManager.skipTwitter(session.userId)
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
                           tweet=tweetHtml, context=u"à série Supernatural",email=app.session_interface.email)


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
    annotation = request.form.get("submit")

    # Save it to ES.
    annManager.annotate(userId, tweetId, annotation)

    # Move to the next tweet.
    flash('Tweet analisado com sucesso!')
    app.logger.info(u'Usuário %s anotou o tweet %s como %s' % (userId, tweetId, annotation))
    return redirect('/')


if __name__ == '__main__':
    with app.app_context():
        current_app.annManager = AnnotationManager(Elasticsearch(['http://localhost:9200']))
    app.session_interface = ElasticSearchSessionInterface(es,"")
    app.run(host='127.0.0.1')
