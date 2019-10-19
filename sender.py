#!/usr/bin/env python3
# coding: utf-8

import socket
import sys
import threading
import time

import utils
from utils import MyTimer, get_runtime, pack_msg, unpack_msg
from tqdm import tqdm

receiver = (sys.argv[1], int(sys.argv[2]))  # (ip, port)
file = sys.argv[3]  # 待传文件

MWS = 100  # MAX_WINDOW_SIZE

TIMEOUT = 3  # seconds

# flags
LAST_SYS_STATE = b'CLOSED'
NEXT_SYS_ACT = b'HS'

# positions
seq = None
ack = None
LastSegSent = None
LastSegAcked = -9999

# statistics
num_seg_sent = 0
num_seg_dropped = 0
num_seg_retransmit = 0
num_ack_duplicate = 0

mutex = threading.Lock()
sender_sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

with open(file, 'rb') as f:
    file_content = f.read()
    print('文件字节数：', len(file_content))
    File_Seg_Count = utils.get_file_seg_count(len(file_content))
    print('文件块数量: ', File_Seg_Count)
    File_Meta_Data = "%s|%s|%s|padding" % (file, len(file_content), utils.md5_hash(file_content))
    print('(文件meta信息：%s)' % File_Meta_Data.rstrip('|padding'))


def get_file_chunk(s):  # s从0开始
    return file_content[s * utils.CHUNK_SIZE:(s + 1) * utils.CHUNK_SIZE]


def retransmit():
    global seq, ack, LastSegSent
    if LastSegAcked == -9999:
        print('重发: HS')
        seq = -2
        segment = pack_msg(seq, -1, b'HS')  # seq: -2
        sender_sk.sendto(segment, receiver)
        return
    if LastSegAcked == -2:
        print('重发: META')
        seq = -1
        segment = pack_msg(seq, ack, b'META', data=File_Meta_Data)  # seq: -1
        sender_sk.sendto(segment, receiver)
        return

    if LastSegAcked + 1 < File_Seg_Count:
        print("重发:", LastSegAcked + 1)
        retry_seq = LastSegAcked + 1
        content = get_file_chunk(retry_seq)
        segment = pack_msg(retry_seq, -1, b'DATA', content)
        sender_sk.sendto(segment, receiver)
        seq = LastSegAcked + 1  #
        LastSegSent = seq  #
    else:
        print('重发: FIN')
        segment = pack_msg(LastSegAcked + 1, -1, b'FIN')
        sender_sk.sendto(segment, receiver)
    if timer.is_alive:
        timer.restart()


timer = MyTimer(TIMEOUT, retransmit)


class MySender(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self.is_connected = False

    def connect(self):
        global NEXT_SYS_ACT, seq, ack, LastSegSent, LAST_SYS_STATE
        if LAST_SYS_STATE != b'CLOSED':
            print('connect(): wrong previous state')  # debug
        while not self.is_connected:
            # handshake
            if LAST_SYS_STATE == b'WRONG':
                if timer.is_alive:
                    timer.cancel()
                sys.exit(1)

            if NEXT_SYS_ACT == b'HS':
                with mutex:
                    seq = -2
                    segment = pack_msg(seq, -1, b'HS')  # seq: -2
                    sender_sk.sendto(segment, receiver)
                    LastSegSent = seq
                    seq += 1
                    NEXT_SYS_ACT = b'WAIT'
                    if not timer.is_alive:
                        timer.start()

            if LAST_SYS_STATE == b'CONNECTED':
                with mutex:
                    self.is_connected = True
                    LAST_SYS_STATE = b'CONNECTED'
                    print("已建立连接")

            time.sleep(0.01)

    @staticmethod
    def meta():
        global seq, ack, file_content, LastSegSent, LastSegAcked, num_seg_sent, num_seg_dropped, File_Seg_Count
        # if LAST_SYS_STATE != b'CONNECTED':
        #     print('meta(): wrong previous state')  # debug
        started = False
        while not started:
            if LAST_SYS_STATE == b'CONNECTED' and NEXT_SYS_ACT == b'META':
                started = True
                with mutex:
                    seq = -1
                    segment = pack_msg(seq, ack, b'META', data=File_Meta_Data)  # seq: -1
                    sender_sk.sendto(segment, receiver)
                    LastSegSent = seq
                    seq += 1  # seq = 0
                    if not timer.is_alive:
                        timer.start()
                    print('文件meta信息已发送\n')
            else:
                time.sleep(0.01)

    @staticmethod
    def transfer_data():
        global seq, ack, file_content, LastSegSent, LastSegAcked, \
            num_seg_sent, num_seg_dropped, File_Seg_Count, LAST_SYS_STATE

        while LAST_SYS_STATE != b'META':
            # print('transfer_data(): wrong previous state')
            time.sleep(0.01)
            pass

        started = False
        while not started:
            if NEXT_SYS_ACT == b'DATA':
                started = True
                with tqdm(total=File_Seg_Count) as pbar:  # 进度条
                    while seq < File_Seg_Count:
                        with mutex:
                            if LastSegSent - LastSegAcked < MWS:
                                # print('正常发：', seq)  # debug
                                # with mutex:
                                segment = pack_msg(seq, ack, b'DATA', data=get_file_chunk(seq))
                                sender_sk.sendto(segment, receiver)
                                num_seg_sent += 1

                                LastSegSent = seq
                                seq += 1
                                pbar.update(1)  # 更新进度条
                        # else:
                        time.sleep(0.0001)
                    LAST_SYS_STATE = b'DATA'
            else:
                time.sleep(0.0001)

    def teardown(self):
        global NEXT_SYS_ACT, seq, ack, file_content, LastSegAcked, LastSegSent, LAST_SYS_STATE
        if LAST_SYS_STATE != b'DATA':
            print('teardown(): wrong previous state')
        while self.is_connected:
            if LastSegAcked >= File_Seg_Count - 1:
                with mutex:
                    if NEXT_SYS_ACT == b'FIN':
                        seq = File_Seg_Count
                        segment = pack_msg(seq, -1, NEXT_SYS_ACT)
                        sender_sk.sendto(segment, receiver)
                        LastSegSent = seq
                        seq += 1
                    elif NEXT_SYS_ACT == b'CLOSED':
                        self.is_connected = False
                        LAST_SYS_STATE = b'CLOSED'
                        NEXT_SYS_ACT = b'HS'
                        print('\n已断开连接')
            else:
                time.sleep(0.001)

    def run(self):
        self.connect()
        self.meta()
        self.transfer_data()
        self.teardown()


class MyAck(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
        self._duplicates = 0
        self._retries = 0

    def run(self):
        global LAST_SYS_STATE, NEXT_SYS_ACT, seq, ack, LastSegSent, LastSegAcked, num_seg_retransmit, num_ack_duplicate

        while True:
            segment = sender_sk.recv(utils.BUF_SIZE)
            segment = unpack_msg(segment)

            with mutex:
                if segment['ack'] < LastSegAcked:  # 更早的重复确认,直接丢弃
                    num_ack_duplicate += 1
                    continue
                if segment['ack'] == LastSegAcked:  # 最新的重复确认，快速重传，最多快速重传2次
                    num_ack_duplicate += 1
                    if segment['command'] == b'DATA_ACK':
                        # print('RE-ACKED: ', segment['ack'])
                        self._duplicates += 1
                        if self._duplicates >= 3 and self._retries < 2:
                            seq = segment['ack'] + 1
                            self._retries += 1
                            self._duplicates = 0
                            retransmit()
                            if timer.is_alive:
                                timer.restart()
                            else:
                                timer.start()
                    continue

                # 正常:
                if segment['ack'] - LastSegAcked > 1:  # 快速确认
                    # print('快速确认：', segment['ack'])  # debug
                    if seq < segment['ack']:
                        seq = segment['ack'] + 1
                        if segment['command'] == b'DATA_ACK' and LAST_SYS_STATE != b'DATA':
                            print('建立连接失败，需要重启接收端！')
                            LAST_SYS_STATE = b'WRONG'
                            sys.exit(1)

                LastSegAcked = segment['ack']  # 可视为“移动窗口”
                ack = segment['seq']

                if segment['command'] == b'HS_ACK':
                    LAST_SYS_STATE = b'CONNECTED'
                    NEXT_SYS_ACT = b'META'
                    continue
                if segment['command'] == b'META_ACK':
                    LAST_SYS_STATE = b'META'
                    NEXT_SYS_ACT = b'DATA'
                    continue

                # data ack
                if segment['command'] == b'DATA_ACK':
                    if LAST_SYS_STATE != b'DATA':
                        LAST_SYS_STATE = b'DATA'
                    # print('ACKED: ', segment['ack'])  # debug
                    self._duplicates = 0
                    self._retries = 0
                    if segment['ack'] <= File_Seg_Count - 1:
                        if segment['ack'] == File_Seg_Count - 1:  # 最后一块的确认
                            NEXT_SYS_ACT = b'FIN'
                            # print('\n收到最后一个文件块的确认')  # debug
                        if LastSegAcked != LastSegSent:
                            if timer.is_alive:
                                timer.restart()
                            else:
                                timer.start()
                        else:
                            timer.cancel()
                    continue

                # teardown ack
                if segment['command'] == b'FIN_ACK':
                    NEXT_SYS_ACT = b'CLOSED'
                    if timer.is_alive:
                        timer.cancel()
                    break


if __name__ == '__main__':
    data_thread = MySender("Sender")
    ack_thread = MyAck("Receiver")

    data_thread.start()
    ack_thread.start()
