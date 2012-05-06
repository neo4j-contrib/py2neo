#!/usr/bin/env python

import os
import sys

import tornado.escape
import tornado.ioloop
import tornado.web

from creole import creole2html

WWW  = os.path.dirname(__file__)
ROOT = os.path.join(WWW, "..")

def read_file(filename):
    try:
        return file(filename).read()
    except IOError:
        raise tornado.web.HTTPError(404)

def get_content(filename):
    content = read_file(filename)
    return creole2html(unicode(content))

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("base.html",
            title="Welcome",
            content=get_content(os.path.join(ROOT, "README.creole"))
        )

class FaviconHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(read_file(os.path.join(ROOT, "art", "py2neo.ico")))

class ArtHandler(tornado.web.RequestHandler):
    def get(self, name):
        self.write(read_file(os.path.join(ROOT, "art", name)))

class StyleHandler(tornado.web.RequestHandler):
    def get(self, name):
        self.set_header("Content-Type", "text/css")
        self.write(read_file(os.path.join(WWW, "style", name)))

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
            content=get_content(os.path.join(ROOT, "tutorials", filename))
        )

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/favicon.ico", FaviconHandler),
    (r"/art/(.*)", ArtHandler),
    (r"/style/(.*)", StyleHandler),
    (r"/tutorials/(.*)", TutorialHandler),
], template_path=os.path.join(WWW, "tmpl"))

if __name__ == "__main__":
    if len(sys.argv) == 0:
        port = 8080
    else:
        port = int(sys.argv[1])
    application.listen(port, address="127.0.0.1")
    tornado.ioloop.IOLoop.instance().start()

