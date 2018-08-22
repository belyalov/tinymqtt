"""
Async Tiny MQTT client
MIT license
(C) Konstantin Belyalov 2018
"""
import gc
import usocket
import uselect
import uasyncio
import uerrno
import logging
import utime as time


log = logging.getLogger('MQTT')

# Since socket is built in module, make it mockable for tests
socket = usocket.socket
getaddrinfo = usocket.getaddrinfo
poll = uselect.poll


class MQTTException(Exception):
    pass


def unhandled_exception(e):
    log.exc(e, "")


class MQTTClient:

    def __init__(self, client_id, keepalive=60, reconnect_timeout=5, clean_session=False):
        self.client_id = client_id
        self.keepalive = keepalive
        self.reconnect_timeout = reconnect_timeout
        self.clean_session = clean_session
        # Current state
        self.connected = False
        self.last_pong = 0
        # Current MsgID
        self.msgid = 0
        # asyncs
        self.loop = uasyncio.get_event_loop()
        self.writer = None
        self.reader_task = None
        self.writer_task = None
        self.ping_task = None
        self.connect_task = None
        self.resumed = False
        # Topics
        self.topics = {}
        # Pending writes queue
        self.pend_writes = []

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

    async def _decode_msglen(self, reader):
        multiplier = 1
        value = 0
        while True:
            digit = (await reader.readexactly(1))[0]
            value += (digit & 0x7f) * multiplier
            multiplier *= 0x80
            if (digit & 0x80) == 0:
                break
        return value

    async def _subscribe(self, topic, qos):
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
        await self.writer.awrite(header)

    async def _publish(self, topic, msg, retain, qos):
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
        await self.writer.awrite(header)

    async def _connect(self):
        # 14 is: ProtoNameLen(2) + ProtoName(6) + ProtoVer(1) + ConnFlags(1) +
        # KeepAlive(2) + ClientLen(2)
        msglen = len(self.client_id) + 14
        header = bytearray(b'\x10') + self._encode_msglen(msglen) + \
            bytearray(b'\x00\x06MQIsdp\x03\x00\x00\x00\x00\x00')
        # Add connection flags
        conn_flags = 0
        if self.clean_session:
            conn_flags |= 1 << 1
        header[-5] = conn_flags
        # Add keepalive
        header[-4:-2] = self.keepalive.to_bytes(2, 'big')
        # Add client ID
        header[-2:] = len(self.client_id).to_bytes(2, 'big')
        header += self.client_id.encode()
        await self.writer.awrite(header)

    async def _ping(self):
        req = b'\xc0\x00'
        await self.writer.awrite(req)

    async def _process_msg(self, reader):
        # Read operation type
        op = (await reader.readexactly(1))[0]
        if op == 0x20:
            # Connect ACK
            mlen = await self._decode_msglen(reader)
            if mlen != 2:
                raise MQTTException('Invalid ConnectACK')
            resp = await reader.readexactly(mlen)
            if resp[1] != 0:
                raise MQTTException('Invalid ConnectACK')
        elif op == 0x90:
            # Subscribe ACK
            mlen = await self._decode_msglen(reader)
            if mlen != 3:
                raise MQTTException('Invalid SubscribeACK')
            resp = await reader.readexactly(mlen)
            # msgid = int.from_bytes(resp[:2], 'big')
        elif op == 0xd0:
            self.last_pong = int(time.time())
            await reader.readexactly(1)
        elif op == 0x30:
            # Incoming message (Publish from server)
            mlen = await self._decode_msglen(reader)
            resp = await reader.readexactly(2)
            tlen = int.from_bytes(resp, 'big')
            topic = await reader.readexactly(tlen)
            msg = await reader.readexactly(mlen - tlen - 2)
            topic = topic.decode()
            msg = msg.decode()
            if topic in self.topics:
                self.topics[topic][0](msg)

    def _schedule_write(self, data, append=True):
        if append:
            self.pend_writes.append(data)
        else:
            self.pend_writes.insert(0, data)
        # Wake up writer task
        if self.writer_task and not self.resumed:
            self.resumed = True
            self.loop.call_soon(self.writer_task)

    async def _connect_task(self):
        while True:
            try:
                sock = socket(usocket.AF_INET, usocket.SOCK_STREAM)
                sock.setblocking(False)
                addr = getaddrinfo(self.server, self.port, 0, usocket.SOCK_STREAM)[0][-1]
                # Make TCP connection
                try:
                    sock.connect(addr)
                except OSError as e:
                    # EINPROGRESS is successful return code for non blocking sockets
                    if e.args[0] != uerrno.EINPROGRESS:
                        raise
                # Wait until socket becomes writable (connected / error)
                yield uasyncio.IOWrite(sock)
                # check for socket errors
                p = poll()
                p.register(sock, uselect.POLLOUT | uselect.POLLERR | uselect.POLLHUP)
                # poll() returns list of tuples (socket, mask)
                evts = p.poll(0)[0][1]
                if evts & (uselect.POLLERR | uselect.POLLHUP):
                    # Since micropython doesn't have "getsockopt()", there is no way
                    # to get actual error code - so raise generic "Connection Aborted"
                    raise OSError(uerrno.ECONNABORTED)

                # Connection established, create coroutines
                log.info("Connected")
                self.connected = True
                self.writer = uasyncio.StreamWriter(sock, {})
                self.reader_task = self._reader_task(uasyncio.StreamReader(sock))
                self.writer_task = self._writer_task()
                self.ping_task = self._ping_task()
                self.loop.call_soon(self.reader_task)
                self.loop.call_soon(self.ping_task)
                # Schedule CONNECT request
                self._schedule_write(self._connect(), append=False)
                # (Re)subscribe to all topics
                for t, v in self.topics.items():
                    # v is tuple (callback, qos)
                    self._schedule_write(self._subscribe(t, v[1]))
                self.connect_task = None
                return
            except uasyncio.CancelledError:
                sock.close()
                return
            except Exception as e:
                # Unable to connect, close everything and wait for next attempt
                log.info("Unable to connect: {}".format(e))
                yield uasyncio.IOWriteDone(sock)
                sock.close()
                self.connected = False
                await uasyncio.sleep(self.reconnect_timeout)

    async def _reader_task(self, reader):
        try:
            # Wait for message, then process it
            while True:
                yield uasyncio.IORead(reader.ios)
                await self._process_msg(reader)
        except (OSError, MQTTException) as e:
            log.info("Connection lost: %s", e)
            self.reconnect()
        except uasyncio.CancelledError:
            return
        except Exception as e:
            log.exc(e, "")
            self.reconnect()
        finally:
            await reader.aclose()

    async def _writer_task(self):
        try:
            while True:
                # cleanup
                self.resumed = False
                # Drain write queue
                while len(self.pend_writes):
                    # Try to send first element from list
                    # After successful send - remove from list
                    await self.pend_writes[0]
                    self.pend_writes.pop(0)
                    gc.collect()
                # Suspend write until next write event
                yield False
        except (OSError, MQTTException) as e:
            log.info("Connection lost: %s", e)
            self.reconnect()
        except uasyncio.CancelledError:
            # Coroutine has been canceled
            return
        except Exception as e:
            log.exc(e, "")
            self.reconnect()

    async def _ping_task(self):
        while True:
            try:
                # Sleep for keepalive timeout, that send ping
                await uasyncio.sleep(self.keepalive)
                self._schedule_write(self._ping())
                # Sleep for half of keepalive and check for PONG from server
                await uasyncio.sleep(self.keepalive // 2)
                if int(time.time()) > self.last_pong + self.keepalive:
                    log.info("Ping timeout")
                    self.reconnect()
            except uasyncio.CancelledError:
                # Coroutine has been canceled
                return
            except Exception as e:
                log.exc(e, "")

    def subscribe(self, topic, cb, qos=0):
        if qos < 0 and qos > 1:
            raise MQTTException('Invalid QOS')
        self.topics[topic] = (cb, qos)
        if self.connected:
            self._schedule_write(self._subscribe(topic, qos))

    def publish(self, topic, msg, retain=False, qos=0):
        if qos < 0 and qos > 1:
            raise MQTTException('Invalid QOS')
        self._schedule_write(self._publish(topic, msg, retain, qos))

    def run(self, server='localhost', port=1883, user='', password=''):
        self.server = server
        self.port = port
        self.user = user
        self.password = password

        self.reconnect()

    def cancel(self, task):
        try:
            if task is not None:
                uasyncio.cancel(task)
        except Exception:
            pass

    def cancel_tasks(self):
        self.cancel(self.connect_task)
        self.connect_task = None
        self.cancel(self.reader_task)
        self.reader_task = None
        self.cancel(self.writer_task)
        self.writer_task = None
        self.cancel(self.ping_task)
        self.ping_task = None

    def reconnect(self):
        # Create and run new connection task
        if self.connect_task is None:
            self.connected = False
            self.cancel_tasks()
            self.connect_task = self._connect_task()
            self.loop.call_soon(self.connect_task)

    def shutdown(self):
        self.cancel_tasks()
