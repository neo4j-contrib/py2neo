from py2neo import Graph
from py2neo.ext.ogm.store2 import Store
from py2neo import watch



class Person(object):
    __pk__ = "email"

    def __init__(self, email, name):
        self.email = email
        self.name = name


def main():
    graph = Graph("http://neo4j:password@localhost:7474/db/data/")
    store = Store(graph)
    store.set_unique("Person", "email")

    alice = Person("alice@example.com", "Alice")
    store.save(alice)

    alice.name = "Alison"
    bob = Person("bob@example.com", "Bob")
    store.save(alice, bob)

    store.relate(alice, "KNOWS", bob)
    store.save(alice, bob)

if __name__ == "__main__":
    #watch("httpstream")
    main()

