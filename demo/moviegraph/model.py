#!/usr/bin/env python
# -*- encoding: utf-8 -*-


from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom


class Movie(GraphObject):
    __primarykey__ = "title"

    title = Property()
    tagline = Property()
    released = Property()

    actors = RelatedFrom("Person", "ACTED_IN")
    directors = RelatedFrom("Person", "DIRECTED")
    producers = RelatedFrom("Person", "PRODUCED")
    comments = RelatedTo("Comment", "COMMENT")

    def __lt__(self, other):
        return self.title < other.title


class Person(GraphObject):
    __primarykey__ = "name"

    name = Property()
    born = Property()

    acted_in = RelatedTo(Movie)
    directed = RelatedTo(Movie)
    produced = RelatedTo(Movie)

    def __lt__(self, other):
        return self.name < other.name


class Comment(GraphObject):

    date = Property()
    name = Property()
    text = Property()

    subject = RelatedFrom(Movie, "COMMENT")

    def __init__(self, date, name, text):
        self.date = date
        self.name = name
        self.text = text

    def __lt__(self, other):
        return self.date < other.date
