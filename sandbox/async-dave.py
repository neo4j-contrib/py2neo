from asyncio import run
from logging import basicConfig, DEBUG

from py2neo.client.aio import Connection

basicConfig(level=DEBUG)


async def main():
    cnt = 0
    query = """
      UNWIND range(1, 1000000) AS row
      RETURN row, [_ IN range(1, 256) | rand()] as fauxEmbedding
    """
    async with Connection("bolt://localhost:7687", auth=("neo4j", "password")) as cx:
        print("Protocol version:", cx.protocol.version)
        print("Connection metadata:", cx.protocol.metadata)
        q = cx.query(query)
        print(q.summary(), q.has_more())
        while q.has_more():
            async for record in q.pull(25000):
                cnt = cnt + 1
            print(f"row {cnt}")


if __name__ == "__main__":
    basicConfig(level=DEBUG)
    run(main())

