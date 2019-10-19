#!/usr/bin/env python3
# coding: utf-8

import hashlib
import math
from threading import Timer
import time
import struct

BUF_SIZE = 2048  # udp buf size
CHUNK_SIZE = 1000  # bytes
PACK_FMT = 'ii8s%ds' % CHUNK_SIZE


def get_file_seg_count(file_size: int):
    return math.ceil(file_size / CHUNK_SIZE)


class MyTimer(object):
    def __init__(self, timeout, func, *args, **kwargs):
        self.timeout = timeout
        self.f = func
        self.args = args
        self.kwargs = kwargs
        self.timer = None
        self.is_alive = False

    def callback(self):
        self.f(*self.args, **self.kwargs)
        self.start()

    def start(self):
        self.timer = Timer(self.timeout, self.callback)
        self.timer.start()
        self.is_alive = True

    def cancel(self):
        self.timer.cancel()
        self.is_alive = False

    def restart(self):
        self.cancel()
        self.start()


def pack_msg(seq, ack, command=b'DATA', data=b'') -> bytes:
    if isinstance(command, str):
        command = command.encode()
    if isinstance(data, str):
        data = data.encode()
    return struct.pack(PACK_FMT, seq, ack, command, data)


def unpack_msg(buf: bytes) -> dict:
    t = struct.unpack(PACK_FMT, buf)
    d = {'seq': t[0], 'ack': t[1], 'command': t[2].rstrip(b'\x00'), 'data': t[3]}
    return d


def md5_hash(contents: bytes) -> str:
    _MD5_TOOL = hashlib.md5()
    _MD5_TOOL.update(contents)
    return _MD5_TOOL.hexdigest()


def get_runtime():
    return round((time.time() - start_time) * 1000, 3)


start_time = time.time()

if __name__ == '__main__':
    msg = 'aabcdabcdabcdabcdabcdabcdabcdabcd'
    raw = pack_msg(101, 102, b'CHECK', msg)
    print(raw)
    print(unpack_msg(raw))
