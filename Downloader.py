#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @author: ZHU Feng

from datetime import datetime, timedelta
import threading
import sys
if sys.version_info[0] == 3:
    from queue import Queue
else:
    from Queue import Queue



PRIORITY_DOWNLOAD_QUEUE = Queue()
COMMON_DOWNLOAD_QUEUE = Queue()

def default_start_predict_time(year_label='y'):
    now = datetime.now()
    today = now.strftime('%'+year_label+'%m%d')
    nowtime = now.strftime('%'+year_label+'%m%d%H')
    if nowtime < today + "12":
        yesterday = now - timedelta(days=1)
        start_predict = yesterday.strftime('%'+year_label+'%m%d') + '20'
    else:
        start_predict = today + '08'
    return start_predict

def produce_items_to_download(start_predict_time):


def download(download_queue):

    while not download_queue.empty():
        pass

def main(m,n):

    start_predict_time = default_start_predict_time()
    produce_items_to_download(start_predict_time)

    for _ in range(m):
        th = threading.Thread(target = download, args=(PRIORITY_DOWNLOAD_QUEUE))
        th.start()

    for _ in range(n):
        th = threading.Thread(target=download,args=(COMMON_DOWNLOAD_QUEUE))
        th.start()

if __name__ == "__main__":
    print('hello world!')