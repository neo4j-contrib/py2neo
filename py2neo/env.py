
import os

from py2neo import ServiceRoot
from py2neo.packages.httpstream.packages.urimagic import URI


NEO4J_HOME = os.getenv("NEO4J_HOME", ".")
NEO4J_URI = URI(os.getenv("NEO4J_URI", ServiceRoot.DEFAULT_URI))
