from asyncio import Protocol, Queue, get_running_loop, run
from collections import deque, namedtuple
from logging import basicConfig, getLogger, DEBUG

from interchange.packstream import pack, unpack

from py2neo import ConnectionProfile
from py2neo.client import bolt_user_agent


log = getLogger(__name__)


Response = namedtuple("Response", ["records", "summary"])


class Bolt(Protocol):

    version = (0, 0)

    def __init__(self):
        self._transport = get_running_loop().create_future()
        self._handshake = get_running_loop().create_future()
        self._buffer = bytearray()
        self._chunks = deque()
        self.responses = deque()
        self.metadata = {}

    def connection_made(self, transport):
        self._transport.set_result(transport)

    def connection_lost(self, exc):
        log.debug('The server closed the connection')
        # TODO: something?

    def data_received(self, data):
        self.__class__ = Bolt4x3
        self._handshake.set_result(data)

    async def handshake(self):
        log.debug("C: <HANDSHAKE>")
        transport = await self._transport
        transport.write(b"\x60\x60\xB0\x17")
        transport.write(b"\x00\x03\x03\x04"
                        b"\x00\x00\x01\x04"
                        b"\x00\x00\x00\x04"
                        b"\x00\x00\x00\x03")
        await self._handshake

    async def _request(self, tag, fields=()):
        log.debug("C: #%02X %r", tag, fields)
        data = bytearray([0xB0 + len(fields), tag])
        for field in fields:
            data.extend(pack(field))
        await self._send_chunk(data)
        await self._send_chunk(b"")

    def _response(self):
        response = Response(Queue(), get_running_loop().create_future())
        self.responses.append(response)
        return response

    async def _send_chunk(self, data):
        size = len(data)
        if size == 0:
            transport = await self._transport
            transport.write(b"\x00\x00")
        elif size < 0x10000:
            transport = await self._transport
            transport.write(bytearray(divmod(size, 0x100)))
            transport.write(data)
        else:
            raise ValueError("Data too long")

    def _process_chunks(self):
        while len(self._buffer) >= 2:
            size = 0x100 * self._buffer[0] + self._buffer[1]
            end = 2 + size
            if len(self._buffer) >= end:
                self._chunks.append(self._buffer[2:end])
                self._buffer = self._buffer[end:]

    def _process_messages(self):
        while b"" in self._chunks:
            message = bytearray()
            while True:
                chunk = self._chunks.popleft()
                if chunk:
                    message.extend(chunk)
                else:
                    break
            size = message[0] - 0xB0
            tag = message[1]
            unpack(message, 2)
            fields = list(unpack(message, 2))
            assert len(fields) == size
            self.message_received(tag, fields)

    def message_received(self, tag, fields):
        log.debug("S: #%02X %.20r", tag, fields)
        if tag == 0x71:
            for field in fields:
                self.responses[0].records.put_nowait(field)
        else:
            response = self.responses.popleft()
            response.summary.set_result((tag, fields))

    async def hello(self, user, password):
        raise NotImplementedError

    def goodbye(self):
        raise NotImplementedError


class Bolt1(Bolt):

    def data_received(self, data):
        self._buffer.extend(data)
        self._process_chunks()
        self._process_messages()

    async def hello(self, user, password):
        raise NotImplementedError  # TODO

    async def goodbye(self):
        pass


class Bolt4x3(Bolt1):

    version = (4, 3)

    async def hello(self, user, password):
        user_agent = bolt_user_agent()
        await self._request(0x01, [{"user_agent": user_agent,
                                    "scheme": "basic",
                                    "principal": user,
                                    "credentials": password}])
        _, summary = self._response()
        tag, fields = await summary
        if tag == 0x70:
            self.metadata.update(fields[0])
        else:
            self._transport.result().close()
            raise ValueError("Failed to authenticate")

    async def goodbye(self):
        await self._request(0x02)

    def run(self, cypher):
        return (self._request(0x10, [cypher, {}, {}]),
                self._response())

    def discard(self, qid=-1):
        return (self._request(0x2F, [{"qid": qid, "n": -1}]),
                self._response())

    def pull(self, qid=-1, n=-1):
        return (self._request(0x3F, [{"qid": qid, "n": n}]),
                self._response())


class Connection:

    def __init__(self, profile=None, **settings):
        self.profile = ConnectionProfile(profile, **settings)
        self.transport = None
        self.protocol = None

    async def __aenter__(self):
        log.debug("Opening connection")
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        log.debug("Closing connection")
        await self.close()

    async def open(self):
        self.transport, self.protocol = await get_running_loop().create_connection(
                lambda: Bolt(), self.profile.host, self.profile.port_number)
        assert isinstance(self.protocol, Bolt)
        await self.protocol.handshake()
        log.debug("Using protocol version %r", self.protocol.version)
        await self.protocol.hello("neo4j", "password")

    async def close(self):
        await self.protocol.goodbye()
        self.transport.close()

    def query(self, cypher):
        return Query(self.protocol, cypher)


class Query:

    def __init__(self, protocol, cypher, qid=-1):
        self._protocol = protocol
        self._request, (_, self._header) = self._protocol.run(cypher)
        self._qid = qid
        self._summary = {}
        self._has_more = True

    def has_more(self):
        return self._has_more

    def summary(self):
        return self._summary

    async def _update_summary(self, summary):
        tag, fields = await summary
        metadata = fields[0]
        if tag == 0x70:
            self._summary.update(metadata)
        else:
            raise Exception("%r: %r" % (tag, metadata))

    async def _run(self):
        if not self._header.done():
            await self._request
            await self._update_summary(self._header)

    async def fields(self):
        await self._run()
        return self._summary["fields"]

    async def pull(self, n=-1):
        await self._run()
        request, (records, summary) = self._protocol.pull(qid=self._qid, n=n)
        await request
        while not records.empty() or not summary.done():
            yield await records.get()
        await self._update_summary(summary)
        self._has_more &= self._summary.pop("has_more", False)

    async def discard(self):
        await self._run()
        request, (_, summary) = self._protocol.discard(qid=self._qid)
        await request
        await self._update_summary(summary)
        self._has_more = False


async def main():
    async with Connection("bolt://localhost:17602") as cx:
        print("Protocol version:", cx.protocol.version)
        print("Connection metadata:", cx.protocol.metadata)
        q = cx.query("UNWIND range(1, 5) AS n RETURN n")
        #print(await q.fields())
        print(q.summary(), q.has_more())
        async for record in q.pull(3):
            print(record)
        print(q.summary(), q.has_more())
        await q.discard()
        print(q.summary(), q.has_more())


if __name__ == "__main__":
    basicConfig(level=DEBUG)
    run(main())

