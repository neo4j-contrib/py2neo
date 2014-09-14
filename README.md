# Py2neo 2.0.beta

## Installation

To install from GitHub, run
```bash
git clone git@github.com:nigelsmall/py2neo.git
cd py2neo
git checkout beta/2.0
pip install .
```

## Hello, Graph

```python
>>> from py2neo import Graph, Node
>>> graph = Graph()
>>> node = Node(hello="world")
>>> graph.create(node)
((n0 {hello:"world"}),)
```
