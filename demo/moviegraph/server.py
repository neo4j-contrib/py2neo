#!/usr/bin/env python
# -*- encoding: utf-8 -*-


from calendar import month_name
from datetime import date
from os import getenv
from os.path import dirname, join as path_join

from demo.moviegraph.bottle import get, post, redirect, request, run, static_file, template, TEMPLATE_PATH
from demo.moviegraph.model import Movie, Person, Comment
from py2neo import Graph
from neo4j.util import watch


home = dirname(__file__)
static = path_join(home, "static")
TEMPLATE_PATH.insert(0, path_join(home, "views"))


# Set up a link to the local graph database.
graph = Graph(password=getenv("NEO4J_PASSWORD"))
watch("neo4j.bolt")


@get('/css/<filename:re:.*\.css>')
def get_css(filename):
    return static_file(filename, root=static, mimetype="text/css")


@get('/images/<filename:re:.*\.png>')
def get_image(filename):
    return static_file(filename, root=static, mimetype="image/png")


@get("/")
def get_index():
    """ Index page.
    """
    return template("index")


@get("/person/")
def get_person_list():
    """ List of all people.
    """
    return template("person_list", people=Person.match(graph).order_by("_.name"))


@get("/person/<name>")
def get_person(name):
    """ Page with details for a specific person.
    """
    person = Person.match(graph, name).first()
    movies = [(movie.title, "Actor") for movie in person.acted_in] + \
             [(movie.title, "Director") for movie in person.directed]
    return template("person", person=person, movies=movies)


@get("/movie/")
def get_movie_list():
    """ List of all movies.
    """
    return template("movie_list", movies=Movie.match(graph).order_by("_.title"))


@get("/movie/<title>")
def get_movie(title):
    """ Page with details for a specific movie.
    """
    return template("movie", movie=Movie.match(graph, title).first())


@post("/movie/comment")
def post_movie_comment():
    """ Capture comment and redirect to movie page.
    """
    today = date.today()
    comment_date = "%d %s %d" % (today.day, month_name[today.month], today.year)
    comment = Comment(comment_date, request.forms["name"], request.forms["text"])

    title = request.forms["title"]
    movie = Movie.match(graph, title).first()
    comment.subject.add(movie)
    graph.create(comment)

    redirect("/movie/%s" % title)


if __name__ == "__main__":
    run(host="localhost", port=8080, reloader=True)
