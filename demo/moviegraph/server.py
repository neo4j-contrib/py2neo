#!/usr/bin/env python
# -*- encoding: utf-8 -*-


from os import getenv
from os.path import dirname, join as path_join
from calendar import month_name
from datetime import date
from py2neo import Graph
from py2neo.ogm import GraphObject, Property, Related, RelatedFrom

from bottle import get, post, redirect, request, run, static_file, template, TEMPLATE_PATH


home = dirname(__file__)
static = path_join(home, "static")
TEMPLATE_PATH.insert(0, path_join(home, "views"))


class Movie(GraphObject):
    __primarykey__ = "title"

    title = Property()
    tagline = Property()
    released = Property()

    actors = RelatedFrom("Person", "ACTED_IN")


class Person(GraphObject):
    __primarykey__ = "name"

    name = Property()
    born = Property()

    acted_in = Related(Movie)
    directed = Related(Movie)
    produced = Related(Movie)


# Set up a link to the local graph database.
graph = Graph(password=getenv("NEO4J_PASSWORD"))


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
    statement = """\
    MATCH (p:Person)
    RETURN p.name AS name
    ORDER BY name
    """
    return template("person_list", people=graph.run(statement))


@get("/person/<name>")
def get_person(name):
    """ Page with details for a specific person.
    """
    person = Person.find_one(graph, name)
    movies = [(movie.title, "Actor") for movie in person.acted_in] + \
             [(movie.title, "Director") for movie in person.directed]
    return template("person", person=person, movies=movies)


@get("/movie/")
def get_movie_list():
    """ List of all movies.
    """
    statement = """\
    MATCH (m:Movie)
    RETURN m.title AS title, m.released AS released
    ORDER BY m.title
    """
    return template("movie_list", movies=graph.run(statement))


@get("/movie/<title>")
def get_movie(title):
    """ Page with details for a specific movie.
    """
    statement = """\
    MATCH (m:Movie) WHERE m.title = {T}
    OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Person)
    OPTIONAL MATCH (m)<-[:DIRECTED]-(d:Person)
    RETURN m.title AS title, m.released AS released,
           collect(a.name) AS actors, d.name AS director
    """
    records = graph.run(statement, T=title)
    title, released, actors, director = records.next()
    statement = """\
    MATCH (m:Movie)-[:COMMENT]->(r:Comment) WHERE m.title = {T}
    RETURN r.name AS name, r.text AS text, r.date AS date
    ORDER BY date DESC
    """
    comments = graph.run(statement, T=title)
    return template("movie", title=title, released=released,
                             actors=actors, director=director, comments=comments)


@post("/movie/comment")
def post_movie_comment():
    """ Capture comment and redirect to movie page.
    """
    title = request.forms["title"]
    name = request.forms["name"]
    text = request.forms["text"]
    today = date.today()
    comment_date = "{d} {m} {y}".format(y=today.year,
                                        m=month_name[today.month],
                                        d=today.day)
    statement = """\
    MATCH (m:Movie) WHERE m.title = {T}
    WITH m
    CREATE (m)-[:COMMENT]->(r:Comment {name:{N},text:{C},date:{D}})
    """
    graph.run(statement, T=title, N=name, C=text, D=comment_date)
    redirect("/movie/%s" % title)


if __name__ == "__main__":
    run(host="localhost", port=8080, reloader=True)
