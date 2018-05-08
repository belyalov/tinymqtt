#!/usr/bin/env micropython
"""
Unittests for async MQTT client
MIT license
(C) Konstantin Belyalov 2018
"""

import unittest
from tinymqtt import MQTTClient
# from uasyncio.core import IORead


class mockReader():
    """Mock for coroutine reader class"""

    def __init__(self, payload):
        self.payload = payload
        self.idx = 0

    async def readexactly(self, n):
        ret = self.payload[self.idx:self.idx + n]
        self.idx += n
        return ret


def run_coro(coro):
    try:
        while True:
            next(coro)
    except StopIteration as si:
        return si.value


class MqttHelpersTests(unittest.TestCase):
    """Unittests for helpers"""
    def setUp(self):
        self.cl = MQTTClient('localhost', 'client')

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
            self.cl.reader = mockReader(r[0])
            val = run_coro(self.cl._decode_msglen())
            self.assertEqual(val, r[1])

    def testConnectMsg(self):
        self.cl.clean_session = True
        msg = self.cl._get_connect_command_msg()
        exp = bytearray(b'\x10\x14\x00\x06MQIsdp\x03\x02\x00<\x00\x06client')
        self.assertEqual(msg, exp)
        # one more - clean session -> false
        self.cl.clean_session = False
        msg = self.cl._get_connect_command_msg()
        exp[11] = 0
        self.assertEqual(msg, exp)

    def testSubscribeMsg(self):
        # topic1 / qos = 1
        msg = self.cl._get_subscribe_command_msg('topic1', 1)
        exp = bytearray(b'\x82\x0b\x00\x01\x00\x06topic1\x01')
        self.assertEqual(msg, exp)
        # try one more time - msgid should be increased by 1
        msg = self.cl._get_subscribe_command_msg('topic1', 1)
        exp[3] = 2
        self.assertEqual(msg, exp)

    def testPublishMsg(self):
        msg = self.cl._get_publish_command_msg('topic1', 'message blah', retain=True, qos=0)
        exp = bytearray(b'1\x14\x00\x06topic1message blah')
        self.assertEqual(msg, exp)
        # one more time with retain / qos changed
        msg = self.cl._get_publish_command_msg('topic1', 'message blah', retain=False, qos=2)
        exp[0] = 0x30 | (2 << 1)
        self.assertEqual(msg, exp)


if __name__ == '__main__':
    unittest.main()
