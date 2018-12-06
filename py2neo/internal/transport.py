

from collections import deque

from py2neo.internal.addressing import get_connection_data


class Runner(object):
    scheme = NotImplemented

    def __new__(cls, uri, **settings):
        cx_data = get_connection_data(uri, **settings)
        subclasses = cls.__subclasses__()
        for subclass in subclasses:
            if subclass.scheme == cx_data["scheme"]:
                inst = object.__new__(subclass)
                return inst
        raise ValueError("Unsupported scheme %r" % cx_data["scheme"])

    def run(self, statement, parameters):
        pass


class BoltRunner(Runner):

    scheme = "bolt"

    def __init__(self, uri, **settings):
        from neobolt.direct import connect, ConnectionPool

        cx_data = get_connection_data(uri, **settings)
        address = cx_data["host"], cx_data["port"]

        def connector(address_, **kwargs):
            return connect(address_, auth=cx_data["auth"], **kwargs)

        self._pool = ConnectionPool(connector, address)

    def run(self, statement, parameters):
        result = Result()

        cx = self._pool.acquire()

        def on_summary():
            result.done = True
            self._pool.release(cx)

        cx.run(statement, parameters, on_success=result.header.update)
        cx.pull_all(on_records=result.records.extend, on_success=result.footer.update, on_summary=on_summary)
        cx.sync()
        return result


class BoltRoutingRunner(Runner):

    scheme = "bolt+routing"

    def __init__(self, uri, **settings):
        from neobolt.direct import connect
        from neobolt.routing import RoutingConnectionPool

        cx_data = get_connection_data(uri, **settings)
        address = cx_data["host"], cx_data["port"]

        def connector(address_, **kwargs):
            return connect(address_, auth=cx_data["auth"], **kwargs)

        self._pool = RoutingConnectionPool(connector, address, {})

    def run(self, statement, parameters):
        result = Result()

        cx = self._pool.acquire()

        def on_summary():
            result.done = True
            self._pool.release(cx)

        cx.run(statement, parameters, on_success=result.header.update)
        cx.pull_all(on_records=result.records.extend, on_success=result.footer.update, on_summary=on_summary)
        cx.sync()
        return result


class HTTPRunner(Runner):

    scheme = "http"

    @classmethod
    def _make_pool(cls, host, port):
        from urllib3 import HTTPConnectionPool
        return HTTPConnectionPool(host=host, port=port)

    def __init__(self, uri, **settings):
        from urllib3 import make_headers

        cx_data = get_connection_data(uri, **settings)

        self._pool = self._make_pool(cx_data["host"], cx_data["port"])
        self._headers = make_headers(basic_auth="neo4j:password")

    def run(self, statement, parameters):
        from json import dumps as json_dumps, loads as json_loads

        r = self._pool.request(method="POST",
                               url="/db/data/transaction/commit",
                               headers=dict(self._headers, **{"Content-Type": "application/json"}),
                               body=json_dumps({
                                   "statements": [
                                       {
                                           "statement": statement,
                                           "parameters": parameters,
                                           "resultDataContents": ["REST"],
                                           "includeStats": True,
                                       }
                                   ]
                               }))
        result = Result()
        data = json_loads(r.data.decode('utf-8'))
        result.header["fields"] = data["results"][0]["columns"]
        result.records = [record["rest"] for record in data["results"][0]["data"]]
        result.footer["stats"] = data["results"][0]["stats"]
        result.done = True
        return result


class HTTPSRunner(HTTPRunner, Runner):

    scheme = "https"

    @classmethod
    def _make_pool(cls, host, port):
        from certifi import where
        from urllib3 import HTTPSConnectionPool
        return HTTPSConnectionPool(host=host, port=port, cert_reqs="CERT_NONE", ca_certs=where())


class Result(object):

    def __init__(self):
        self.header = {}
        self.records = deque()
        self.footer = {}
        self.done = False


def main():
    from sys import argv
    uri = argv[1]
    runner = Runner(uri, auth=("neo4j", "password"))
    result = runner.run("UNWIND range(1, $x) AS n RETURN n, n * n AS n_sq", {"x": 3})
    while not result.done:
        pass
    print(result.header)
    for record in result.records:
        print(record)
    print(result.footer)


if __name__ == "__main__":
    main()
