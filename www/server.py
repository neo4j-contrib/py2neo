#!/usr/bin/env python

import os

import tornado.escape
import tornado.ioloop
import tornado.web

from creole import creole2html

def get_content(filename):
    content = file(filename).read()
    return creole2html(unicode(content))

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("base.html",
            title="Welcome",
            content=get_content(os.path.join("..", "README.creole"))
        )

class FaviconHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(file(os.path.join("..", "art", "py2neo.ico")).read())

class ArtHandler(tornado.web.RequestHandler):
    def get(self, name):
        self.write(file(os.path.join("..", "art", name)).read())

class StyleHandler(tornado.web.RequestHandler):
    def get(self, name):
        self.set_header("Content-Type", "text/css")
        self.write(file(os.path.join("styles", name)).read())

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/favicon.ico", FaviconHandler),
    (r"/art/(.*)", ArtHandler),
    (r"/styles/(.*)", StyleHandler),
], template_path="tmpl")

if __name__ == "__main__":
    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()

