

def test_simple_evaluation(graph):
    value = graph.evaluate("RETURN 1")
    assert value == 1


def test_simple_evaluation_with_parameters(graph):
    value = graph.evaluate("RETURN $x", x=1)
    assert value == 1


def test_run_and_consume_multiple_records(graph):
    cursor = graph.run("UNWIND range(1, 3) AS n RETURN n")
    record = next(cursor)
    assert record[0] == 1
    record = next(cursor)
    assert record[0] == 2
    record = next(cursor)
    assert record[0] == 3
