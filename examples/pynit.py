#!/usr/bin/env python

# Copyright 2011 Nigel Small
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# 	http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
PynIT!
=======
Example application implementing a simple bookmarking/URL-shortening service.
Each node within the database represents a bookmark, with a short handle (e.g.
"aB23x") and a web address. Suggest using Poster to add bookmarks.

GET http://localhost:5000/
View a list of all bookmarks in the database

GET http://localhost:5000/<handle>
Redirect (301) to the address behind the specified handle

POST http://localhost:5000/ {"address":"http://neo4j.org/"}
Add a new bookmark pointing to the specified address

"""

import random

from py2neo import neo4j
from flask import Flask, abort, json, redirect, render_template_string, request
app = Flask(__name__)

# Base HTML template for displaying index page
INDEX_TEMPLATE = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN">
<html lang="en">
<head>
	<title>Index of Bookmarks</title>
</head>
	<body>
	<h1>Index of Bookmarks</h1>
	<dl id="bookmarks">
	{% for bm in bookmarks %}
		<dt><a href="/{{ bm.handle }}">{{ bm.handle }}</a></dt>
		<dd><a href="{{ bm.address }}">{{ bm.address }}</a></dd>
	{% endfor %}
	</dl>
	</body>
</html>
"""

# List of safe characters to use when building handles
HANDLE_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

# Set up links to the database, subreference node and node index
bm_db     = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
bm_subref = bm_db.get_subreference_node("BOOKMARKS")
bm_index  = bm_db.get_node_index("bookmarks")

# Retrieve the node behind the specified handle or None if not used
def get_bookmark_node(handle):
	b = bm_index.search("handle", handle)
	return b[0] if len(b) > 0 else None

# Obtain a randomly generated handle which is not already within the index
def get_random_handle(size):
	while True:
		# generate a ramdom handle of the given size
		handle = "".join([
			random.choice(HANDLE_CHARS)
			for i in range(size)
		])
		# return it if it doesn't exist in the index
		if get_bookmark_node(handle) is None:
			return handle

# Handle calls to the index page
@app.route("/", methods=["GET", "POST"])
def index():
	if request.method == "POST":
		# add a new bookmark
		if request.json is not None:
			# use the JSON data passed into the post request
			data = {
				"handle": get_random_handle(5),
				"address": request.json["address"]
			}
			# create a new node for this bookmark in the database
			node = bm_db.create_node(data)
			# add this node to the index
			bm_index.add(node, "handle", data["handle"])
			# create a link from the subreference node to this new node
			bm_subref.create_relationship_to(node, "BOOKMARK")
			# finally, return some data containing the bookmark handle
			return json.dumps(data)
		else:
			# no JSON passed to POST so signal an invalid request
			abort(400)
	else:
		# return a list of bookmarks using the index template
		return render_template_string(
			INDEX_TEMPLATE,
			bookmarks=bm_db.get_properties(*bm_subref.get_related_nodes(
				neo4j.Direction.OUTGOING,
				"BOOKMARK"
			))
		)

# Resolve calls to a particular handle
@app.route("/<handle>")
def resolve(handle):
	# grab the node behind this handle
	bm_node = get_bookmark_node(handle)
	if bm_node is not None:
		# perform a 301 redirect to the bookmark's web address
		return redirect(bm_node["address"], code=301)
	else:
		# handle not found, throw a 404
		abort(404)

if __name__ == "__main__":
	app.run()

