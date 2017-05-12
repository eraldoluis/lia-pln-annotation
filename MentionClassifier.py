#!/usr/bin/env python
# -*- coding: utf-8 -*-
from random import randint
from flask import Flask,url_for,render_template,request,session
from flask.sessions import SessionInterface,SessionMixin
from werkzeug.datastructures import CallbackDict
from elasticsearch import Elasticsearch
import requests
import time
from uuid import uuid4

app = Flask(__name__)

class ElasticsearchSession(CallbackDict, SessionMixin):
    def __init__(self, initial=None, sid=None,client=Elasticsearch(['http://localhost:9200'])):
        self.storedSession = initial
        self.sid = sid
        self.client = client
        self.modified = False
        self.mentionIndex = randint(0,4999)
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
    })

class ElasticSearchSessionInterface(SessionInterface):
    def __init__(self,index ='test'):
        self.client = Elasticsearch(['http://localhost:9200'])

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if sid:
            try:
                stored_session = self.client.get(index='test',doc_type='anotadores',id=sid)
                return ElasticsearchSession(initial=stored_session['_source']['anotacoes'],
                                            sid=stored_session['_id'])
            except:
                print "Usuario nao encontrado\n"
        sid = str(uuid4())
        return ElasticsearchSession(sid=sid)


    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if session is None:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            return
        try:
            self.client.get(index='test',doc_type='anotadores',id=session.sid)
        except:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            self.client.index(index="test", doc_type="anotadores",id=session.sid, body={"anotacoes": [] })
            expires = time.time() + 3650 * 24 * 3600
            response.set_cookie(app.session_cookie_name,session.sid,
                            expires=time.strftime("%a, %d-%b-%Y %T GMT", time.gmtime(expires)),
                            httponly=True, domain=domain)


@app.route('/MentionClassificated', methods=['GET', 'POST'])
def MentionClassifier():
    if request.form["answer"] == "1":
        anotacao =  "Relevante"
    elif request.form["answer"] == "2":
        anotacao = "Nao Relevante"
    else:
        anotacao = "Nao sei"
    try:
        anotations = session.storedSession
        anotations.append({"tw_id":session.results["hits"]["hits"][session.mentionIndex]["_source"]["tweet"]["id_str"] , "anotacao": anotacao})
        session.client.update(index="test", doc_type="anotadores", id=session.sid,
                      body={"doc":{"anotacoes": anotations }})
        return MentionGetter(Analised=True)
    except Exception as e:
        print session.mentionIndex
        return str(e)

@app.route('/')
def MentionGetter(Analised=False):
    tweet_text = "https://twitter.com/"+session.results["hits"]["hits"][session.mentionIndex]["_source"]["tweet"]["user"]["screen_name"]+"/status/"+session.results["hits"]["hits"][session.mentionIndex]["_source"]["tweet"]["id_str"]
    r = requests.get(("https://publish.twitter.com/oembed?url="+tweet_text)).content
    re = r.decode('utf-8').replace("\\", "")
    retweet = ""

    if re.find('Sorry, you are not authorized to see this status.') != -1:
        finaldois = session.results["hits"]["hits"][session.mentionIndex]["_source"]["tweet"]["text"] + '\n'
    else:
        if session.results["hits"]["hits"][session.mentionIndex]["_source"]["tweet"]["text"][0:2] == "RT":
    	    retweet = "Um retweet de @%s do tweet:\n" % session.results["hits"]["hits"][session.mentionIndex]["_source"]["tweet"]["user"]["screen_name"]
        ree = re.replace("u003C","<")
        reee = ree.replace("u003E",">")
        reeee = reee.replace("blockquote>n<script","blockquote><script")
        T = reeee.rsplit("\"html\":\"", 1)
        try:
            indice = T[1].index("script>\"")
        except:
            session.mentionIndex = randint(0,4999)
            return MentionGetter(Analised=False)
        final = T[1][:indice+7]
        indicede = final.index("><p lang")
        finaldois = final[:indicede] + " \"data-lang=\"en\"" + final[indicede:]
    #print  app.session_interface.client.search(index=['test'],doc_type=['anotadores'])['hits']['hits']

   
    return render_template('MentionDisplay.html',rt = retweet,block=finaldois)



if __name__ == '__main__':
    app.session_interface = ElasticSearchSessionInterface()
    app.run(host='127.0.0.1')
    

