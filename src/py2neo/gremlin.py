#!/usr/bin/env python

# Copyright 2011 Nigel Small
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

"""
Gremlin utility module
"""

__author__    = "Nigel Small <py2neo@nigelsmall.org>"
__copyright__ = "Copyright 2011 Nigel Small"
__license__   = "Apache License, Version 2.0"


def execute(script, graph_db):
	"""
	Executes the supplied script using the Gremlin plugin, if available.

	@param script: a string containing the Gremlin script to execute
	@raise NotImplementedError: if the Gremlin plugin is not available
	@return: the result of the Gremlin script

	"""
	if graph_db._gremlin_uri is None:
		raise NotImplementedError("Gremlin functionality not available")
	else:
		response = graph_db._post(graph_db._gremlin_uri, {'script': script})
		return response
