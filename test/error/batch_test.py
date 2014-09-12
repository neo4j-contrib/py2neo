
from py2neo import WriteBatch, CypherJob, BatchError, GraphError


def test_invalid_syntax_raises_cypher_error(graph):
    batch = WriteBatch(graph)
    batch.append(CypherJob("X"))
    try:
        batch.submit()
    except BatchError as error:
        assert isinstance(error, BatchError)
        cause = error.__cause__
        assert isinstance(cause, GraphError)
        assert cause.__class__.__name__ == "SyntaxException"
        assert cause.exception == "SyntaxException"
        assert cause.fullname in [None, "org.neo4j.cypher.SyntaxException"]
    else:
        assert False
