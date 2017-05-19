# coding=utf-8
import time
from uuid import uuid4

from flask.sessions import SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict


class ElasticsearchSession(CallbackDict, SessionMixin):
    def __init__(self, userId):
        super(ElasticsearchSession, self).__init__()
        self.userId = userId


class ElasticSearchSessionInterface(SessionInterface):
    def __init__(self, es):
        self.es = es

    def open_session(self, app, request):
        userId = request.cookies.get(app.session_cookie_name)
        if userId is None:
            userId = str(uuid4())
            self.es.index(index="test", doc_type="anotadores", id=userId, body={})
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

