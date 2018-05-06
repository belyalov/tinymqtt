#!/usr/bin/env micropython
"""
Unittests for async MQTT client
MIT license
(C) Konstantin Belyalov 2018
"""

import unittest
from tinymqtt import MQTTClient
# from uasyncio.core import IORead


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


if __name__ == '__main__':
    unittest.main()
