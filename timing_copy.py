#!/usr/bin/env python
import sys
import csv
import time
import threading
import argparse

def worker(list_file, source_dir, dest_dir, count):

    print("list_file = ",list_file)
    print("source_dir = ",source_dir)
    print("dest_dir = ",dest_dir)
    print("count = ",count)
    time.sleep(8)

def scheduler(interval, list_file, source_dir, dest_dir, f, wait = True):
    base_time = time.time()
    next_time = 0
    count = 0
    while True:
        count += 1
        t = threading.Thread(target=f, args=([list_file, source_dir, dest_dir, count]))
        t.start()
        if wait:
            t.join()
        next_time = ((base_time - time.time()) % interval) or interval
        time.sleep(next_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--interval", type=int, help="file copy interval")
    parser.add_argument("-l", "--list_file", type=str, help="file list")
    parser.add_argument("-s", "--source_dir", type=str, help="source directory")
    parser.add_argument("-d", "--dest_dir", type=str, help="dest directory")
    args = parser.parse_args()
    interval = args.interval
    list_file = args.list_file
    source_dir = args.source_dir
    dest_dir = args.dest_dir

    scheduler(interval, list_file, source_dir, dest_dir, worker, False)