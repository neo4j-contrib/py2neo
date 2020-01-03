#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from os import getenv
from os.path import dirname, join as path_join

from flask import Flask, render_template, redirect, request

from py2neo import Graph

from py2neo.demo.movies.model import Movie, Person


app = Flask(__name__)
home = dirname(__file__)
static = path_join(home, "static")


# Set up a link to the local graph database.
graph = Graph(password=getenv("NEO4J_PASSWORD"))


@app.route("/")
def get_index():
    """ Index page.
    """
    return render_template("index.html")


@app.route("/person/")
def get_person_list():
    """ List of all people.
    """
    return render_template("person_list.html", people=Person.match(graph).order_by("_.name"))


@app.route("/person/<name>")
def get_person(name):
    """ Page with details for a specific person.
    """
    person = Person.match(graph, name).first()
    movies = [(movie.title, "Actor") for movie in person.acted_in] + \
             [(movie.title, "Director") for movie in person.directed]
    return render_template("person.html", person=person, movies=movies)


@app.route("/movie/")
def get_movie_list():
    """ List of all movies.
    """
    return render_template("movie_list.html", movies=Movie.match(graph).order_by("_.title"))


@app.route("/movie/<title>")
def get_movie(title):
    """ Page with details for a specific movie.
    """
    return render_template("movie.html", movie=Movie.match(graph, title).first())


@app.route("/movie/review", methods=["POST"])
def post_movie_review():
    """ Capture review and redirect to movie page.
    """
    with graph.begin() as tx:
        reviewer = Person.match(graph, request.values["name"]).first()
        if reviewer is None:
            reviewer = Person()
            reviewer.name = request.values["name"]
            tx.create(reviewer)
        movie = Movie.match(graph, request.values["title"]).first()
        movie.reviewers.add(reviewer, summary=request.values["summary"], rating=request.values["rating"])
        tx.push(movie)
    return redirect("/movie/%s" % movie.title)
