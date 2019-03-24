import copy
import time
from asyncio import Lock

import tornado.tcpserver
import tornado.ioloop
import tornado.gen
from tornado.iostream import StreamClosedError


class Source:
    statuses = {
        1: "IDLE",
        2: "ACTIVE",
        3: "RECHARGE"
    }

    def __init__(self, id):
        self.id = id
        self.last_message_num = 0
        self.status = None
        self.last_message_ts = None
        self.lock = Lock()


class TestServer:

    def __init__(self):
        self.sources = {}
        self.listeners = {}
        self.sources_server = SourcesServer(self)
        self.listeners_server = ListenerServer(self)

    def start(self, host):
        self.sources_server.listen(8888, host)
        self.listeners_server.listen(8889, host)


class SourcesServer(tornado.tcpserver.TCPServer):

    def __init__(self, base_server):
        super().__init__()
        self.base_server = base_server

    async def handle_stream(self, stream, address):
        while True:
            try:
                data = bytearray(await stream.read_bytes(13))
                header = data[0]
                message_num = int.from_bytes(data[1:3], byteorder='big', signed=False)
                source_id = data[3:11].decode()
                source_status = data[11]
                numfields = data[12]
                fields = []
                for i in range(numfields):
                    field = await stream.read_bytes(12)
                    data.extend(field)
                    fields.append((field[0:8].decode(), int.from_bytes(field[8:12], byteorder='big', signed=False)))
                xor = await stream.read_bytes(1)
                if xor[0] == await self.calc_lrc(data):
                    answer = await self.prepare_answer(b'\x11', data[1:3])

                    if not self.base_server.sources.get(source_id):
                        self.base_server.sources[source_id] = Source(source_id)
                    source = self.base_server.sources[source_id]

                    async with source.lock:
                        source.status = Source.statuses[source_status]
                        source.last_message_num = message_num
                        source.last_message_ts = time.time()

                    self.base_server.listeners_server.send_fields_to_all_listeners(source_id, fields)
                else:
                    answer = await self.prepare_answer(b'\x12', b'\x00')
                await stream.write(answer)
            except Exception as e:
                if not isinstance(e, StreamClosedError):
                    await stream.write(await self.prepare_answer(b'\x12', b'\x00'))
                else:
                    break

    async def prepare_answer(self, header, message_num):
        answer = bytearray(header)
        answer.extend(message_num)
        answer.append(await self.calc_lrc(answer))
        return answer

    async def calc_lrc(self, data):
        lrc = 0
        for b in data:
            lrc ^= b
        return lrc


class ListenerServer(tornado.tcpserver.TCPServer):
    def __init__(self, base_server):
        super().__init__()
        self.base_server = base_server

    async def handle_stream(self, stream, address):
        self.base_server.listeners[address] = stream
        self.on_listener_connect(address)

    def on_listener_connect(self, address):
        try:
            now = time.time()
            for s in copy.copy(list(self.base_server.sources.values())):
                ts_dist = int((now - s.last_message_ts) * 100)
                source = "[{}] {} | {} | {}\r\n".format(s.id, s.last_message_num, s.status, ts_dist)
                self.base_server.listeners[address].write(source.encode())
        except StreamClosedError:
            del self.base_server.listeners[address]

    def send_fields_to_all_listeners(self, source_id, fields):
        for k, s in copy.copy(list(self.base_server.listeners.items())):
            try:
                for f in fields:
                    s.write("[{}] {} | {}\r\n".format(source_id, f[0], f[1]).encode())
            except StreamClosedError:
                del self.base_server.listeners[k]
                continue


def main():
    host = '127.0.0.1'

    server = TestServer()

    server.start(host)
    print("Listening on %s..." % host)

    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
