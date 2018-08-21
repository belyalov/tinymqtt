#!/usr/bin/env micropython
"""
Unittests for async MQTT client
MIT license
(C) Konstantin Belyalov 2018
"""

import utime
import logging
import unittest
import uasyncio
import uselect
import uerrno
import tinymqtt
from tinymqtt import MQTTClient


# Exception to be raised by MockSocketConnect()
ConnectException = OSError(uerrno.EINPROGRESS)
# Result returned by poll()
PollResult = 0


def mock_getaddrinfo(*args):
    return [(1, 1), (1, 1)]


class MockPoll():
    """Mock for select.poll()"""

    def register(self, *args):
        pass

    def poll(self, *args):
        return [(0, PollResult)]


class MockReader():
    """Mock for coroutine reader class"""

    def __init__(self, payload, error=None):
        self.payload = payload
        self.error = error
        self.idx = 0

    async def readexactly(self, n):
        if self.error:
            raise self.error
        ret = self.payload[self.idx:self.idx + n]
        self.idx += n
        return ret


class MockWriter():
    """Mock for coroutine writer class"""

    def __init__(self, generate_expection=None):
        """
        keyword arguments:
            generate_expection - raise exception when calling send()
        """
        self.s = 1
        self.history = []
        self.closed = False
        self.generate_expection = generate_expection

    def clear(self):
        self.history = []

    async def awrite(self, buf, off=0, sz=-1):
        if sz == -1:
            sz = len(buf) - off
        if self.generate_expection:
            raise self.generate_expection
        # Save biffer into history - so to be able to assert then
        self.history.append(buf[:sz])

    async def aclose(self):
        self.closed = True


class MockSocket():
    def __init__(self, *args):
        self.connect_called = False
        self.close_called = False

    def setblocking(self, v):
        pass

    def connect(self, *args):
        self.connect_called = True
        raise ConnectException

    def close(self, *args):
        self.close_called = True


# Mock asyncio loop
class MockLoop():
    def __init__(self, *args):
        self.tasks = []
        self.queue = []

    def create_task(self, task):
        self.tasks.append(task)

    def call_later(self, interval, task):
        self.queue.append((interval, task))

    def call_soon(self, task):
        self.queue.append((0, task))

    def clear(self):
        self.queue = []

    def pop(self):
        when, task = self.queue.pop(0)
        return task

    def pop_run(self):
        when, task = self.queue.pop(0)
        return next(task)

    def queue_len(self):
        return len(self.queue)


def run_coro(coro):
    try:
        while True:
            next(coro)
    except StopIteration as si:
        return si.value


def drain_writer(task):
    while True:
        res = next(task)
        if res is False:
            break


class MqttHelpersTests(unittest.TestCase):
    """Unittests for helpers"""

    def setUp(self):
        self.cl = MQTTClient('client', server='localhost')
        self.cl.writer = MockWriter()

    def testEncodeMsgLength(self):
        runs = [(0, b'\x00'),
                (1, b'\x01'),
                (127, b'\x7f'),
                (128, b'\x80\x01'),
                (255, b'\xff\x01'),
                (255, b'\xff\x01'),
                ]
        for x, exp in runs:
            self.assertEqual(self.cl._encode_msglen(x), exp)

    def testDecodeMsgLength(self):
        runs = [(b'\x00', 0),
                (b'\x01', 1),
                (b'\x7f', 127),
                (b'\x80\x01', 128),
                (b'\xff\x01', 255),
                (b'\xff\x01', 255),
                ]
        for r in runs:
            self.cl.reader = MockReader(r[0])
            val = run_coro(self.cl._decode_msglen())
            self.assertEqual(val, r[1])

    def testConnectMsg(self):
        self.cl.clean_session = True
        run_coro(self.cl._connect())
        exp = bytearray(b'\x10\x14\x00\x06MQIsdp\x03\x02\x00<\x00\x06client')
        self.assertEqual(self.cl.writer.history[0], exp)
        # one more - clean session -> false
        self.cl.clean_session = False
        run_coro(self.cl._connect())
        exp[11] = 0
        self.assertEqual(self.cl.writer.history[1], exp)

    def testSubscribeMsg(self):
        run_coro(self.cl._subscribe('topic1', 1))
        exp = bytearray(b'\x82\x0b\x00\x01\x00\x06topic1\x01')
        self.assertEqual(self.cl.writer.history[0], exp)
        # try one more time - msgid should be increased by 1
        run_coro(self.cl._subscribe('topic1', 1))
        exp[3] = 2
        self.assertEqual(self.cl.writer.history[1], exp)

    def testPublishMsg(self):
        run_coro(self.cl._publish('topic1', 'message blah', retain=True, qos=0))
        exp = bytearray(b'1\x14\x00\x06topic1message blah')
        self.assertEqual(self.cl.writer.history[0], exp)
        # one more time with retain / qos changed
        run_coro(self.cl._publish('topic1', 'message blah', retain=False, qos=2))
        exp[0] = 0x30 | (2 << 1)
        self.assertEqual(self.cl.writer.history[1], exp)


class MqttTests(unittest.TestCase):

    def cb(self):
        self.cb_called = True

    def setUp(self):
        logging._level = logging.WARNING
        # Mock socket.socket()
        self.cb_called = False
        tinymqtt.client.socket = MockSocket
        tinymqtt.client.poll = MockPoll
        tinymqtt.client.getaddrinfo = mock_getaddrinfo
        self.loop = uasyncio.get_event_loop()
        self.loop.clear()
        self.cl = MQTTClient('client', server='localhost')

    def testConnect(self):
        # It is OK to subscribe / publish msg when connection is not active
        self.cl.subscribe('dummy', self.cb)
        self.cl.publish('dummy', '1')
        # Ensure that only one connect_task in queue after run()
        self.cl.run()
        self.assertEqual(self.loop.queue_len(), 1)
        # Run "happy path" where connection can be established, Expect IOWrite to be yielded
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        # Resume task, emulating that connection established
        with self.assertRaises(StopIteration):
            next(task)
        # Ensure that connect_task cleaned up
        self.assertIsNone(self.cl.connect_task)
        # Now we have connection established, there should be 3 tasks scheduled to run
        # and 2 pending messages to be sent
        self.assertEqual(self.loop.queue, [
            (0, self.cl.reader_task),
            (0, self.cl.ping_task),
            (0, self.cl.writer_task),
        ])
        self.assertEqual(len(self.cl.pend_writes), 3)
        self.assertTrue(self.cl.connected)

        # Drain write queue (Mock writer before)
        writer = MockWriter()
        self.cl.writer = writer
        drain_writer(self.cl.writer_task)
        # Check send history
        self.assertEqual(len(writer.history), 3)
        # CONNECT / PUBLISH / SUBSCRIBE. You may noticed that order has slightly
        # changed - this is due to:
        # 1. CONNECT message is always first
        # 2. Then previously scheduled messages
        # 3. All active subscriptions
        self.assertEqual(writer.history[0][0], 0x10)    # CONNECT
        self.assertEqual(writer.history[1][0], 0x30)    # PUBLISH
        self.assertEqual(writer.history[2][0], 0x82)    # SUBSCRIBE
        # Writer should reset flag - to make itself able to wake up
        self.assertEqual(self.cl.resumed, False)

        # Test subscribe to one more topic when connection is alive
        self.loop.queue.clear()
        writer.clear()
        self.cl.subscribe("topic", self.cb)
        self.assertEqual(len(self.cl.pend_writes), 1)
        self.assertEqual(self.loop.queue_len(), 1)
        # Send updates
        drain_writer(self.cl.writer_task)
        self.assertEqual(len(writer.history), 1)
        self.assertEqual(writer.history[0][0], 0x82)    # SUBSCRIBE

        # Test publish when connection is alive
        self.loop.queue.clear()
        writer.clear()
        self.cl.publish("topic1", "val1")
        self.cl.publish("topic2", "val2")
        self.assertEqual(len(self.cl.pend_writes), 2)
        self.assertEqual(self.loop.queue_len(), 1)
        # Send updates
        drain_writer(self.cl.writer_task)
        self.assertEqual(len(writer.history), 2)
        self.assertEqual(writer.history[0][0], 0x30)    # PUBLISH
        self.assertEqual(writer.history[1][0], 0x30)

    def testConnectFailed(self):
        # First scenario - test when connect() function returns error
        global ConnectException
        ConnectException = OSError(uerrno.ECONNABORTED)
        self.cl.run()
        # Make 2 attempts to "connect"
        task = self.loop.pop()
        for z in range(2):
            res = next(task)
            # expect task to yield with asyncio.sleep(5)
            self.assertEqual(res, 5000)
            self.assertIsNotNone(self.cl.connect_task)
        self.assertFalse(self.cl.connected)

        # Second scenario - poll() returns error
        ConnectException = OSError(uerrno.EINPROGRESS)
        global PollResult
        PollResult = uselect.POLLERR
        res = next(task)
        # connect() succeed
        self.assertEqual(uasyncio.IOWrite, type(res))
        res = next(task)
        self.assertEqual(res, 5000)
        self.assertFalse(self.cl.connected)

        # Now let it be connected
        PollResult = 0
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        self.assertTrue(self.cl.connected)

        # Final check - tasks / pending msgs
        self.assertEqual(self.loop.queue, [
            (0, self.cl.reader_task),
            (0, self.cl.ping_task),
            (0, self.cl.writer_task),
        ])
        self.assertEqual(len(self.cl.pend_writes), 1)

    def testConnectionResetReader(self):
        # Make connection
        self.cl.run()
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        self.loop.clear()
        self.assertTrue(self.cl.connected)

        # Mock reader / writer. Reader set to raise exception
        self.cl.reader = MockReader('123', error=OSError(uerrno.ECONNABORTED))
        self.cl.writer = MockWriter()

        # Run all tasks once
        res = next(self.cl.reader_task)
        self.assertEqual(uasyncio.IORead, type(res))
        res = next(self.cl.writer_task)
        self.assertEqual(False, res)
        res = next(self.cl.ping_task)
        self.assertEqual(self.cl.keepalive * 1000, res)

        # Wakeup reader and emulate read error
        with self.assertRaises(StopIteration):
            res = next(self.cl.reader_task)
        self.assertFalse(self.cl.connected)
        # Reader task finished. Before exit it should call reconnect() method:
        self.assertIsNone(self.cl.reader_task)
        self.assertIsNone(self.cl.writer_task)
        self.assertIsNone(self.cl.ping_task)
        self.assertIsNotNone(self.cl.connect_task)
        # All canceled tasks should be scheduled to final run
        self.assertEqual(self.loop.queue_len(), 3)
        # Run all canceled tasks (except last one - connect_task)
        for idx in range(len(self.loop.queue) - 1):
            t = self.loop.pop()
            with self.assertRaises(StopIteration):
                next(t)
        # One task should remain - connect_task
        self.assertEqual(self.loop.queue_len(), 1)

        # Reconnect
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        # Check if new "connection" established
        self.assertIsNone(self.cl.connect_task)
        self.assertEqual(self.loop.queue, [
            (0, self.cl.reader_task),
            (0, self.cl.ping_task),
            (0, self.cl.writer_task),
        ])
        self.assertEqual(len(self.cl.pend_writes), 1)
        self.assertTrue(self.cl.connected)

    def testConnectionResetWriter(self):
        # Make connection
        self.cl.run()
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        # Mock writer.
        self.cl.writer = MockWriter()
        self.assertTrue(self.cl.connected)

        # Run all tasks once
        res = next(self.cl.reader_task)
        self.assertEqual(uasyncio.IORead, type(res))
        res = next(self.cl.writer_task)
        self.assertEqual(False, res)
        res = next(self.cl.ping_task)
        self.assertEqual(self.cl.keepalive * 1000, res)

        # Set Mock Writer set to raise exception
        self.cl.writer = MockWriter(OSError(uerrno.ECONNABORTED))

        # Publish message and wakeup writer with writer error emulation
        self.cl.publish('dummy', 'val')
        self.loop.clear()
        with self.assertRaises(StopIteration):
            res = next(self.cl.writer_task)
        self.assertFalse(self.cl.connected)
        # Since writer finished with error publish should remain in write queue:
        self.assertEqual(len(self.cl.pend_writes), 1)
        # Check that writer scheduled reconnection before exit:
        self.assertIsNone(self.cl.reader_task)
        self.assertIsNone(self.cl.writer_task)
        self.assertIsNone(self.cl.ping_task)
        self.assertIsNotNone(self.cl.connect_task)
        # All canceled tasks should be scheduled to final run
        self.assertEqual(self.loop.queue_len(), 3)
        # Run all canceled tasks (except last one - connect_task)
        for idx in range(len(self.loop.queue) - 1):
            t = self.loop.pop()
            with self.assertRaises(StopIteration):
                next(t)
        # One task should remain - connect_task
        self.assertEqual(self.loop.queue_len(), 1)

        # Reconnect
        self.cl.writer = MockWriter()
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        self.assertTrue(self.cl.connected)
        # Check if new "connection" established
        self.assertIsNone(self.cl.connect_task)
        self.assertEqual(self.loop.queue, [
            (0, self.cl.reader_task),
            (0, self.cl.ping_task),
            (0, self.cl.writer_task),
        ])
        # 2 pending messages: CONNECT + PUBLISH (unsend before)
        self.assertEqual(len(self.cl.pend_writes), 2)

    def testPingTimeout(self):
        # Make connection
        self.cl.run()
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        self.assertTrue(self.cl.connected)
        # Mock writer.
        self.cl.writer = MockWriter()

        # Run all tasks once
        res = next(self.cl.reader_task)
        self.assertEqual(uasyncio.IORead, type(res))
        res = next(self.cl.writer_task)
        self.assertEqual(False, res)
        res = next(self.cl.ping_task)
        self.assertEqual(self.cl.keepalive * 1000, res)

        # Resume ping task - expect PING message sent
        res = next(self.cl.ping_task)
        self.assertEqual(self.cl.keepalive // 2 * 1000, res)
        # print("\n!!", self.loop.queue)
        # _ping message in queue present
        self.assertEqual(len(self.cl.pend_writes), 1)
        # Emulate that we've got PONG response
        self.cl.last_pong = utime.time()
        res = next(self.cl.ping_task)
        # Connection should remain active, no mo messages
        self.assertTrue(self.cl.connected)
        self.assertEqual(len(self.cl.pend_writes), 1)
        self.loop.clear()

        # Next ping task cycle - emulate ping timeout
        res = next(self.cl.ping_task)
        # Drain writer (2 message to be "sent")
        self.assertEqual(len(self.cl.pend_writes), 2)
        drain_writer(self.cl.writer_task)
        # Emulate pong timeout
        self.cl.last_pong = 0
        res = next(self.cl.ping_task)
        # Reconnection should be scheduled
        self.assertFalse(self.cl.connected)

        # All canceled tasks should be scheduled to final run
        self.assertEqual(self.loop.queue_len(), 3)
        # Run all canceled tasks (except last one - connect_task)
        for idx in range(len(self.loop.queue) - 1):
            t = self.loop.pop()
            with self.assertRaises(StopIteration):
                next(t)
        # One task should remain - connect_task
        self.assertEqual(self.loop.queue_len(), 1)

        # Reconnect
        self.cl.writer = MockWriter()
        task = self.loop.pop()
        res = next(task)
        self.assertEqual(uasyncio.IOWrite, type(res))
        with self.assertRaises(StopIteration):
            next(task)
        # Check if new "connection" established
        self.assertIsNone(self.cl.connect_task)
        self.assertEqual(self.loop.queue, [
            (0, self.cl.reader_task),
            (0, self.cl.ping_task),
            (0, self.cl.writer_task),
        ])
        # 1 pending message: CONNECT
        self.assertEqual(len(self.cl.pend_writes), 1)


if __name__ == '__main__':
    # Create mock event loop
    uasyncio.core._event_loop_class = MockLoop
    unittest.main()
