Command Line Tool
=================

Py2neo provides a generic command line tool which taps into some of the
functionality available within the library. This tool can be used directly from
the Python module::

    python -m py2neo.tool --help

Alternatively, the simpler script wrapper can be used::

    neotool --help

The general form for commands is::

    neotool [<options>] <command> <args>
    
General options are available to change the database connection used. These
are::

    -S/--scheme <scheme>  Set the URI scheme, e.g. (http, https)
    -H/--host <host>      Set the URI host
    -P/--port <port>      Set the URI port

The set of commands available via Neotool are outlined below:

Clearing the Database
---------------------
::

    neotool clear

.. warning::

    This will remove all nodes and relationships from the graph database
    (including the reference node).

Cypher Execution
----------------
::

    neotool cypher "start n=node(1) return n, n.name?"

Cypher queries passed will be executed and the results returned in an ASCII art
table, such as the one below::

     n                    | n.name?
    ----------------------+---------
     (1 {"name":"Alice"}) | Alice

Delimited Output
~~~~~~~~~~~~~~~~
::

    neotool cypher-csv "start n=node(1) return n.name, n.age?"
    neotool cypher-tsv "start n=node(1) return n.name, n.age?"

The results of Cypher queries can instead be returned as comma or tab separated
values. Each value is encoded as JSON, so strings are output between double
quotes whereas numbers and booleans are not. Arrays may also be output.

Although it is possible to output nodes and relationships, this format is most
useful when returning individual properties.

Comma delimited output looks similar to that below::

    "n.name","n.age?"
    "Alice",34

JSON Output
~~~~~~~~~~~
::

    neotool cypher-json "start n=node(1) return n"

The ``cypher-json`` command outputs query results within a JSON object. Nodes
and relationships within the results produce nested objects similar to, but
different from, the raw REST API results returned by the server.

From the command line, output can be pretty printed by piping through the
python ``json.tool`` module::

    neotool cypher-json "start n=node(1) return n" | python -m json.tool

Example output is below (not pretty printed by default)::

    [
        {
            "n": {
                "properties": {
                    "age": 34, 
                    "name": "Alice"
                }, 
                "uri": "http://localhost:7474/db/data/node/1"
            }
        }
    ]

Geoff Output
~~~~~~~~~~~~
::

    neotool cypher-geoff "start n=node(1) return n"

Nodes and relationships can be output in Geoff format using the
``cypher-geoff`` command. Individual properties cannot be output with this
method.

Inserting/Merging Geoff Data
----------------------------
::

    neotool geoff-insert example.geoff
    neotool geoff-merge example.geoff

Converting XML Data into Geoff
------------------------------
::

    neotool xml-geoff example.xml

Namespaces in more complex XML files are suppressed by default. The example
below shows the output from conversion of an XBRL file that is included as a
test file within the project source code::

    $ python -m py2neo.tool xml-geoff test/files/statement.xbrl
    (node_1 {"OtherAdministrativeExpenses":35996000000,"OtherAdministrativeExpenses contextRef":"J2004","OtherAdministrativeExpenses decimals":0,"OtherAdministrativeExpenses unitRef":"EUR","OtherOperatingExpenses":870000000,"OtherOperatingExpenses contextRef":"J2004","OtherOperatingExpenses decimals":0,"OtherOperatingExpenses unitRef":"EUR","OtherOperatingIncomeTotalByNature":10430000000,"OtherOperatingIncomeTotalByNature contextRef":"J2004","OtherOperatingIncomeTotalByNature decimals":0,"OtherOperatingIncomeTotalByNature unitRef":"EUR","OtherOperatingIncomeTotalFinancialInstitutions":38679000000,"OtherOperatingIncomeTotalFinancialInstitutions contextRef":"J2004","OtherOperatingIncomeTotalFinancialInstitutions decimals":0,"OtherOperatingIncomeTotalFinancialInstitutions unitRef":"EUR","schemaRef href":"http://www.org.com/xbrl/taxonomy","schemaRef type":"simple"})
    (BJ2004)
    (node_3 {"identifier":"ACME","identifier scheme":"www.iqinfo.com/xbrl"})
    (node_4 {"instant":"2004-01-01"})
    (EJ2004)
    (node_6 {"identifier":"ACME","identifier scheme":"www.iqinfo.com/xbrl"})
    (node_7 {"instant":"2004-12-31"})
    (J2004)
    (node_9 {"identifier":"ACME","identifier scheme":"www.iqinfo.com/xbrl"})
    (node_10 {"endDate":"2004-12-31","startDate":"2004-01-01"})
    (EUR {"measure":"iso4217:EUR"})
    (node_1)-[:context]->(BJ2004)
    (BJ2004)-[:entity]->(node_3)
    (BJ2004)-[:period]->(node_4)
    (node_1)-[:context]->(EJ2004)
    (EJ2004)-[:entity]->(node_6)
    (EJ2004)-[:period]->(node_7)
    (node_1)-[:context]->(J2004)
    (J2004)-[:entity]->(node_9)
    (J2004)-[:period]->(node_10)
    (node_1)-[:unit]->(EUR)

Namespace prefixes can however be supplied on the command line. These are
prefixed to relationship type names in the output::

    $ python -m py2neo.tool xml-geoff test/files/statement.xbrl xbrli="http://www.xbrl.org/2003/instance"
    (node_1 {"OtherAdministrativeExpenses":35996000000,"OtherAdministrativeExpenses contextRef":"J2004","OtherAdministrativeExpenses decimals":0,"OtherAdministrativeExpenses unitRef":"EUR","OtherOperatingExpenses":870000000,"OtherOperatingExpenses contextRef":"J2004","OtherOperatingExpenses decimals":0,"OtherOperatingExpenses unitRef":"EUR","OtherOperatingIncomeTotalByNature":10430000000,"OtherOperatingIncomeTotalByNature contextRef":"J2004","OtherOperatingIncomeTotalByNature decimals":0,"OtherOperatingIncomeTotalByNature unitRef":"EUR","OtherOperatingIncomeTotalFinancialInstitutions":38679000000,"OtherOperatingIncomeTotalFinancialInstitutions contextRef":"J2004","OtherOperatingIncomeTotalFinancialInstitutions decimals":0,"OtherOperatingIncomeTotalFinancialInstitutions unitRef":"EUR","schemaRef href":"http://www.org.com/xbrl/taxonomy","schemaRef type":"simple"})
    (BJ2004)
    (node_3 {"xbrli_identifier":"ACME","xbrli_identifier scheme":"www.iqinfo.com/xbrl"})
    (node_4 {"xbrli_instant":"2004-01-01"})
    (EJ2004)
    (node_6 {"xbrli_identifier":"ACME","xbrli_identifier scheme":"www.iqinfo.com/xbrl"})
    (node_7 {"xbrli_instant":"2004-12-31"})
    (J2004)
    (node_9 {"xbrli_identifier":"ACME","xbrli_identifier scheme":"www.iqinfo.com/xbrl"})
    (node_10 {"xbrli_endDate":"2004-12-31","xbrli_startDate":"2004-01-01"})
    (EUR {"xbrli_measure":"iso4217:EUR"})
    (node_1)-[:xbrli_context]->(BJ2004)
    (BJ2004)-[:xbrli_entity]->(node_3)
    (BJ2004)-[:xbrli_period]->(node_4)
    (node_1)-[:xbrli_context]->(EJ2004)
    (EJ2004)-[:xbrli_entity]->(node_6)
    (EJ2004)-[:xbrli_period]->(node_7)
    (node_1)-[:xbrli_context]->(J2004)
    (J2004)-[:xbrli_entity]->(node_9)
    (J2004)-[:xbrli_period]->(node_10)
    (node_1)-[:xbrli_unit]->(EUR)

