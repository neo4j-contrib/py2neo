#!/usr/bin/env python

import os
import sys

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
        self.write(file(os.path.join("style", name)).read())

class TutorialHandler(tornado.web.RequestHandler):
    def get(self, name):
        title = " ".join([
            word[0].upper() + word[1:]
            for word in name.split("-")
        ])
        filename = "_".join([
            word.lower()
            for word in name.split("-")
        ]) + ".creole"
        self.render("base.html",
            title=title,
            content=get_content(os.path.join("..", "tutorials", filename))
        )

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/favicon.ico", FaviconHandler),
    (r"/art/(.*)", ArtHandler),
    (r"/style/(.*)", StyleHandler),
    (r"/tutorials/(.*)", TutorialHandler),
], template_path="tmpl")

if __name__ == "__main__":
    if len(sys.argv) == 0:
        port = 8080
    else:
        port = int(sys.argv[1])
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()

