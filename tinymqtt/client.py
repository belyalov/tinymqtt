"""
Async Tiny MQTT client
MIT license
(C) Konstantin Belyalov 2018
"""
import usocket as socket
import uselect as select
import uasyncio as asyncio
from uasyncio.synchro import Lock
import uerrno as errno
import sys
import logging
import utime as time


log = logging.getLogger('MQTT')


class MQTTException(Exception):
    pass


def unhandled_exception(e):
    log.exc(e, "")


class Config():
    """Variable MQTT configuration.
    Very basic class - intended to be easily replaced with your own.
    """

    def add_param(self, name, default, validator=None, callback=None, group=None):
        setattr(self, name, default)


class MQTTClient:

    def __init__(self, client_id, server='localhost', port=1883, user='', password='',
                 keepalive=60, reconnect_timeout=5, clean_session=False, config=None):
        self.cfg = config
        if not self.cfg:
            self.cfg = Config()
        self.cfg.add_param('mqtt_server', server, group='mqtt', callback=self.reconnect)
        self.cfg.add_param('mqtt_port', port, group='mqtt')
        self.cfg.add_param('mqtt_user', user, group='mqtt')
        self.cfg.add_param('mqtt_password', password, group='mqtt')
        self.cfg.add_param('mqtt_keepalive', keepalive, group='mqtt')

        self.reconnect_timeout = reconnect_timeout
        self.client_id = client_id
        self.clean_session = clean_session
        # Current state
        self.connected = False
        # Current MsgID
        self.msgid = 0
        # Socket
        self._sock = None
        # asyncs
        self.loop = None
        self.receiver_task = None
        self.ping_task = None
        self.writer_task = None
        # Connection lock
        self.conn_lock = Lock()
        # Topics
        self.topics = {}
        # Pending subscribe / publish / etc requests (list of cores)
        self.pending_writes = []

    def _encode_msglen(self, x):
        ret = bytearray(4)
        i = 0
        while True:
            digit = x % 128
            x //= 128
            if x > 0:
                digit |= 0x80
            ret[i] = digit
            i += 1
            if x <= 0:
                return ret[:i]

    async def _decode_msglen(self):
        multiplier = 1
        value = 0
        while True:
            digit = (await self.reader.readexactly(1))[0]
            value += (digit & 0x7f) * multiplier
            multiplier *= 0x80
            if (digit & 0x80) == 0:
                break
        return value

    def _get_subscribe_command_msg(self, topic, qos):
        # 5 is: MsgID(2) + TopicLen(2) + QOSlen(1)
        msglen = 5 + len(topic)
        header = bytearray(b'\x82') + self._encode_msglen(msglen) + bytearray(b'\x00\x00\x00\x00')
        # msgid
        self.msgid += 1
        header[-4:-2] = self.msgid.to_bytes(2, 'big')
        # topic len
        header[-2:] = len(topic).to_bytes(2, 'big')
        # topic
        header += topic.encode()
        # QoS
        header += qos.to_bytes(1, 'big')
        return header

    def _get_publish_command_msg(self, topic, msg, retain, qos):
        # TODO: add proper QoS support
        # 2 is: TopicLen(2)
        msglen = 2 + len(topic) + len(msg)
        header = bytearray(b'\x30') + self._encode_msglen(msglen) + bytearray(b'\x00\x00')
        # Flags
        flags = int(retain)
        flags |= qos << 1
        header[0] |= flags
        # topic len
        header[-2:] = len(topic).to_bytes(2, 'big')
        # topic
        header += topic.encode()
        # message
        header += msg.encode()
        return header

    def _get_connect_command_msg(self):
        # 14 is: ProtoNameLen(2) + ProtoName(6) + ProtoVer(1) + ConnFlags(1) +
        # KeepAlive(2) + ClientLen(2)
        msglen = 14
        msglen += len(self.client_id)
        header = bytearray(b'\x10') + self._encode_msglen(msglen) + \
            bytearray(b'\x00\x06MQIsdp\x03\x00\x00\x00\x00\x00')
        # Add connection flags
        conn_flags = 0
        if self.clean_session:
            conn_flags |= 1 << 1
        header[-5] = conn_flags
        # Add keepalive
        header[-4:-2] = self.cfg.mqtt_keepalive.to_bytes(2, 'big')
        # Add client ID
        header[-2:] = len(self.client_id).to_bytes(2, 'big')
        header += self.client_id.encode()
        return header

    async def _connect(self):
        try:
            if self._sock:
                self._sock.close()
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            addr = socket.getaddrinfo(self.cfg.mqtt_server, self.cfg.mqtt_port, 0, socket.SOCK_STREAM)[0][-1]
            self._sock.setblocking(False)
            # Make TCP connection
            try:
                self._sock.connect(addr)
            except OSError as e:
                if e.args[0] != errno.EINPROGRESS:
                    raise
            # Wait until socket becomes writable (connected / error)
            yield asyncio.IOWrite(self._sock)
            # check for socket errors
            p = select.poll()
            p.register(self._sock, select.POLLOUT | select.POLLERR | select.POLLHUP)
            # poll() returns list of tuples (socket, mask)
            evts = p.poll(0)[0][1]
            if evts & (select.POLLERR | select.POLLHUP):
                # Since micropython doesn't have "getsockopt()", there is no way
                # to get actual error code - so raise generic "Connection Aborted"
                raise OSError(errno.ECONNABORTED)
            self.connected = True
            self.last_pong = int(time.time())
            self.reader = asyncio.StreamReader(self._sock)
            self.writer = asyncio.StreamWriter(self._sock, {})
            log.info("Connected")
            # Connected!
            # Send CONNECT command before main cycle
            await self.writer.awrite(self._get_connect_command_msg())
            # Wake up writer process
            self.loop.call_soon(self.writer_task)
        except Exception as e:
            self.connected = False
            self._sock.close()
            self._sock = None
            raise e

    async def _subscribe(self, topic, qos):
        await self.writer.awrite(self._get_subscribe_command_msg(topic, qos))

    async def _publish(self, topic, msg, retain, qos):
        await self.writer.awrite(self._get_publish_command_msg(topic, msg, retain, qos))

    async def _ping(self):
        req = b'\xc0\x00'
        await self.writer.awrite(req)

    async def _close(self):
        if self._sock is None:
            return
        yield asyncio.IOWriteDone(self._sock)
        self._sock.close()
        self._sock = None

    async def _process_msg(self):
        # Read operation type
        op = (await self.reader.readexactly(1))[0]
        if op == 0x20:
            # Connect ACK
            mlen = await self._decode_msglen()
            if mlen != 2:
                raise MQTTException('Invalid ConnectACK')
            resp = await self.reader.readexactly(mlen)
            if resp[1] != 0:
                raise MQTTException('Invalid ConnectACK')
        elif op == 0x90:
            # Subscribe ACK
            mlen = await self._decode_msglen()
            if mlen != 3:
                raise MQTTException('Invalid SubscribeACK')
            resp = await self.reader.readexactly(mlen)
            msgid = int.from_bytes(resp[:2], 'big')
        elif op == 0xd0:
            self.last_pong = int(time.time())
            await self.reader.readexactly(1)
        elif op == 0x30:
            # Incoming message (Publish from server)
            mlen = await self._decode_msglen()
            resp = await self.reader.readexactly(2)
            tlen = int.from_bytes(resp, 'big')
            topic = await self.reader.readexactly(tlen)
            msg = await self.reader.readexactly(mlen - tlen - 2)
            topic = topic.decode()
            msg = msg.decode()
            if topic in self.topics:
                self.topics[topic](msg)

    def _schedule_write(self, coro):
        self.pending_writes.append(coro)
        # Wake up writer
        if self.loop:
            self.loop.call_soon(self.writer_task)

    async def _ping_task(self):
        while True:
            try:
                await asyncio.sleep(self.cfg.mqtt_keepalive // 2)
                if not self.connected:
                    continue
                # Check for ping timeout
                now = int(time.time())
                if now > self.last_pong + self.cfg.mqtt_keepalive:
                    self.reconnect(msg="PING timeout")
                    continue
                # Send one more ping
                self._schedule_write(self._ping())
            except asyncio.CancelledError:
                # Coroutine has been canceled
                return
            except Exception as e:
                log.exc(e, "")

    async def _receiver_task(self):
        try:
            while True:
                # Hold lock until get connected to broker
                await self.conn_lock.acquire()
                try:
                    await self._close()
                    while True:
                        try:
                            await self._connect()
                            break
                        except Exception as e:
                            log.error(e)
                            await asyncio.sleep(self.reconnect_timeout)
                finally:
                    self.conn_lock.release()
                # Main loop (while connection alive)
                while True:
                    try:
                        # Wait for message
                        yield asyncio.IORead(self._sock)
                        if not self.connected:
                            break
                        # Process message
                        await self._process_msg()
                    except (OSError, MQTTException) as e:
                        log.info("Connection lost")
                        self.connected = False
                        # restart connection loop
                        break
                    except Exception as e:
                        log.exc(e, "")
        except asyncio.CancelledError:
            await self._close()

    async def _writer_task(self):
        try:
            while True:
                await self.conn_lock.acquire()
                try:
                    while len(self.pending_writes):
                        coro = self.pending_writes.pop()
                        await coro
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.reconnect(msg="Write failed")
                finally:
                    self.conn_lock.release()
                # Pause coroutine until resumed manually
                yield False
        except asyncio.CancelledError:
            # Coroutine has been canceled
            return

    def subscribe(self, topic, cb, qos=0):
        if qos not in [0, 1]:
            raise MQTTException('Invalid QOS')
        self.topics[topic] = cb
        self._schedule_write(self._subscribe(topic, qos))

    def publish(self, topic, msg, retain=False, qos=0):
        if qos not in [0, 1]:
            raise MQTTException('Invalid QOS')
        self._schedule_write(self._publish(topic, msg, retain, qos))

    def run(self, loop=None):
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        self.receiver_task = self._receiver_task()
        self.ping_task = self._ping_task()
        self.writer_task = self._writer_task()
        self.loop.create_task(self.receiver_task)
        self.loop.create_task(self.ping_task)

    def reconnect(self, msg='User Request'):
        if not self.connected:
            return
        self.connected = False
        self.receiver_task.pend_throw(MQTTException(msg))
        self.loop.call_soon(self.receiver_task)

    def shutdown(self):
        if self.receiver_task:
            asyncio.cancel(self.receiver_task)
        if self.ping_task:
            asyncio.cancel(self.ping_task)
        # In case of now connection established this could fail, ignoring
        try:
            asyncio.cancel(self.writer_task)
        except Exception:
            pass
