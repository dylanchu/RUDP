#!/usr/bin/env python3
# coding: utf-8

from tqdm import tqdm

import socket
import sys
import utils
from utils import pack_msg, unpack_msg

port = sys.argv[1]
host = ("0.0.0.0", int(port))
print('Receiver：开始监听 %s 端口...' % port)

# buffer to store data
buf = []
num_seg_rcvd = 0

seq = -1
Last_Acked = None  # 已确认对方包的号
File_Meta_Data = -1

receiver_sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
receiver_sk.bind(host)

while 1:
    raw_data, address = receiver_sk.recvfrom(utils.BUF_SIZE)
    rcv_data = unpack_msg(raw_data)
    # print('收到消息类型：', rcv_data['command'])
    ack = rcv_data['seq']
    # handshake
    if rcv_data['command'] == b'HS':  # 收到第一次握手
        seq = 0
        segment = pack_msg(seq, ack, b'HS_ACK')
        receiver_sk.sendto(segment, address)
        Last_Acked = ack
        continue

    if rcv_data['command'] == b'META':
        seq = rcv_data['ack'] + 1
        tmp_str = rcv_data['data'].rstrip(b'\x00').decode()
        if tmp_str != '':
            File_Meta_Data = tmp_str.rstrip('|padding')
            print('收到文件meta信息： %s\n' % File_Meta_Data)
            segment = pack_msg(seq, ack, b'META_ACK')
            receiver_sk.sendto(segment, address)
            Last_Acked = ack
            break

file_name, file_size, file_md5 = File_Meta_Data.split('|')
file_size = int(file_size)
File_Seg_Count = utils.get_file_seg_count(file_size)

time_start = utils.get_runtime()

with tqdm(total=File_Seg_Count) as pbar:
    while 1:
        raw_data, address = receiver_sk.recvfrom(utils.BUF_SIZE)
        rcv_data = unpack_msg(raw_data)
        ack = rcv_data['seq']

        if ack != Last_Acked + 1:  # 未按序到达,重发上次确认报文
            segment = pack_msg(seq, Last_Acked, b'DATA_ACK')
            receiver_sk.sendto(segment, address)
            continue

        # receive data
        if rcv_data['command'] == b'META':
            seq = rcv_data['ack'] + 1
            tmp_str = rcv_data['data'].rstrip(b'\x00').decode()
            if tmp_str != '':
                File_Meta_Data = tmp_str.rstrip('|padding')
                print('收到文件meta信息： %s' % File_Meta_Data)
                segment = pack_msg(seq, ack, b'META_ACK')
                receiver_sk.sendto(segment, address)
                Last_Acked = ack
                continue
        if rcv_data['command'] == b'DATA':
            # print(ack)  # 打印包序号
            buf.append(rcv_data['data'])  # 已经保证有序，直接放入即可
            pbar.update(1)  # 更新进度条
            num_seg_rcvd += 1
            seq = rcv_data['ack'] + 1
            segment = pack_msg(seq, ack, b'DATA_ACK')
            receiver_sk.sendto(segment, address)
            Last_Acked = ack
            continue

        # teardown
        if rcv_data['command'] == b'FIN':
            # print('收到FIN')
            seq = rcv_data['ack'] + 1
            segment = pack_msg(seq, ack, b'FIN_ACK')
            receiver_sk.sendto(segment, address)
            Last_Acked = ack
            break

# save file
print()
time_end = utils.get_runtime()
print('传输用时：%.2f ms' % (time_end - time_start))
print()
print('文件块数量：', len(buf))
print('原文件meta信息： 文件名 %s|大小(Bytes) %s|MD5 %s' % (file_name, file_size, file_md5))
data = b''.join(buf)[:file_size]
calc_md5 = utils.md5_hash(data)
print('md5校验结果：', calc_md5, end='\t')
if calc_md5 == file_md5:
    print('通过')
    print('写入文件中...')
    file_new_name = 'received_' + file_name
    with open(file_new_name, 'wb') as f:
        f.write(data)
    print('文件已保存为： %s' % file_new_name)
else:
    print('不通过')
    print('传输文件失败！')
