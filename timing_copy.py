#!/usr/bin/env python
import sys
import time
import threading
import argparse
import pandas as pd
import os.path
import shutil

def worker(file_list, source_dir, dest_dir, p_cycle_num):
    for file_item in file_list:
        source_path = os.path.join(source_dir, file_item)
        dest_path = os.path.join(dest_dir, file_item)
        shutil.copyfile(source_path, dest_path)
    time.sleep(8)

def scheduler(interval, list_file, source_dir, dest_dir, f, wait = True):
    base_time = time.time()
    next_time = 0
    v_cycle_num = 0
    df = pd.read_csv(list_file)
    max_cycle = df.cycle_num.max()
    while True:
        v_cycle_num += 1
        if v_cycle_num > max_cycle:
            break
        new_df = df[df.cycle_num == v_cycle_num]
        if len(new_df) == 0:
            continue
        t = threading.Thread(target=f, args=([new_df.file_name, source_dir, dest_dir, v_cycle_num]))
        t.start()
        if wait:
            t.join()
        next_time = ((base_time - time.time()) % interval) or interval
        time.sleep(next_time)
    
    print("End.")
    quit()

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