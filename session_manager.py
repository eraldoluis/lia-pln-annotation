# coding=utf-8
import time
from datetime import datetime
from uuid import uuid4

from dateutil import tz
from elasticsearch.client import IndicesClient
from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict


class ElasticsearchSession(CallbackDict, SessionMixin):
    def __init__(self, es, index, docType, userId=None):
        super(ElasticsearchSession, self).__init__()

        self.es = es
        self.index = index
        self.docType = docType

        self.userId = userId

        self.userEmail = None
        self.lastLogin = None
        self.signup = None

        if self.userId is None:
            self.userId = str(uuid4())
        else:
            self.__findEmailById()

    def login(self, email):
        hits = self.es.search(index=self.index, doc_type=self.docType, body={
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "email": email
                            }
                        }
                    ]
                }
            }
        })["hits"]["hits"]

        if len(hits) > 0:
            # User has logged before with this email.
            hit = hits[0]
            self.userEmail = email
            self.userId = hit['_id']
            user = hit['_source']
            self.signup = user['signup']
            self.lastLogin = datetime.now(tz.tzlocal())
        else:
            # New user.
            # self.userId = str(uuid4())
            self.userEmail = email
            self.signup = datetime.now(tz.tzlocal())
            self.lastLogin = self.signup

        # Update Elasticsearch.
        self.es.index(index=self.index, doc_type=self.docType, id=self.userId,
                      body={
                          "email": self.userEmail,
                          "lastLogin": self.lastLogin,
                          "signup": self.signup
                      })

    def logout(self):
        self.userId = str(uuid4())
        self.userEmail = None
        self.lastLogin = None
        self.signup = None

    def __findEmailById(self):
        res = self.es.get(index=self.index, doc_type=self.docType, id=self.userId, ignore=404)
        if res['found']:
            self.userEmail = res['_source']['email']


class ElasticsearchSessionInterface(SessionInterface):
    def __init__(self, es, index, docType):
        self.es = es
        self.index = index
        self.docType = docType
        self.__checkIndexAndType()

    def open_session(self, app, request):
        userId = request.cookies.get(app.session_cookie_name)
        return ElasticsearchSession(es=self.es, index=self.index, docType=self.docType, userId=userId)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if session is None:
            response.delete_cookie(app.session_cookie_name, domain=domain)
            return

        expires = time.time() + 3650 * 24 * 3600
        response.set_cookie(app.session_cookie_name, session.userId,
                            expires=time.strftime("%a, %d-%b-%Y %T GMT", time.gmtime(expires)),
                            httponly=True, domain=domain)

    def __checkIndexAndType(self):
        """
        Check if the given index and type exist. If the doc type or the index do not exist, create them and the
        corresponding mappings.

        :return:
        """
        ic = IndicesClient(self.es)
        if not ic.exists(index=self.index):
            ic.create(index=self.index)

        if not ic.exists_type(index=self.index, doc_type=self.docType):
            # Create type.
            ic.put_mapping(index=self.index, doc_type=self.docType, body={
                "properties": {}
            })

        mapping = ic.get_mapping(index=self.index, doc_type=self.docType)
        properties = mapping.values()[0]["mappings"][self.docType]
        if len(properties) == 0:
            ic.put_mapping(index=self.index, doc_type=self.docType, body={
                "properties": {
                    "email": {
                        "type": "keyword"
                    },
                    "signup": {
                        "type": "date"
                    },
                    "lastLogin": {
                        "type": "date"
                    }
                }
            })

        return True
