#!/usr/bin/env python
# -*- coding=utf-8 -*-
###########################################################################
# Copyright (C) 2013-2022 by Caspar. All rights reserved.
# File Name: onedrive.py
# Author: Shankai Yan
# E-mail: dr.skyan@gmail.com
# Created Time: 2022-01-20 06:33:21
###########################################################################
#

import os, time
import subprocess, queue, threading

RCLONE_VERSION = '1.57.0'
DEFAULT_CONN_NAME = 'onedrive'
ONEDRIVE_PATH = '/content/%s' % DEFAULT_CONN_NAME

def mkdir(path):
  if path and not os.path.exists(path):
    print(("Creating folder: " + path))
    os.makedirs(path)

os.system('wget https://downloads.rclone.org/v%s/rclone-v%s-linux-amd64.deb && sudo apt install ./rclone-v%s-linux-amd64.deb' % (RCLONE_VERSION, RCLONE_VERSION, RCLONE_VERSION))
os.system('rm ./rclone-v%s-linux-amd64.deb' % RCLONE_VERSION)
DATA_ROOT_PATH = os.path.join(ONEDRIVE_PATH, 'notebooks/data')


class InteractiveCMD(object):
    def __init__(self, cmd):
    	self.cmd = cmd
    	self.p = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    	self.out_queue = queue.Queue()
    	self.err_queue = queue.Queue()

    def start(self, verbose=False):
        out_thread = threading.Thread(target=InteractiveCMD._enqueue_output, args=(self.p.stdout, self.out_queue))
        err_thread = threading.Thread(target=InteractiveCMD._enqueue_output, args=(self.p.stderr, self.err_queue))
        out_thread.daemon = True
        err_thread.daemon = True
        out_thread.start()
        err_thread.start()
        if verbose:
            print(InteractiveCMD._get_output(self.out_queue))
            print(InteractiveCMD._get_output(self.err_queue))

    def input(self, input_str, verbose=False):
        if verbose: print('Your input: %s' % input_str)
        self.p.stdin.write(input_str.strip('\n')+'\n')
        self.p.stdin.flush()
        if verbose:
            self.print_output_error()

    def inputs(self, inputs, intervel=0, verbose=False):
    	for input_str in inputs:
    		self.input(input_str, verbose=verbose)
    		time.sleep(intervel)

    def print_output_error(self):
        print(InteractiveCMD._get_output(self.out_queue))
        print(InteractiveCMD._get_output(self.err_queue))

    def _enqueue_output(out, tgt_queue):
    	for line in iter(out.readline, b''):
    		tgt_queue.put(line)
    	out.close()


    def _get_output(out_queue):
    	out_str = ''
    	try:
    		while True:
    			out_str += out_queue.get_nowait()
    	except queue.Empty:
    		return out_str

def mount(verbose=False):
    mount_prefix = input('Please input the mount location [default: /content]:').rstrip('/') or '/content'
    conn_name = input('Please input a connection name [default: %s]:' % DEFAULT_CONN_NAME) or DEFAULT_CONN_NAME
    config_token = input('Please input your config token: \nHint: You may get it by executing `rclone authorize "onedrive"` \n')
    cmd = InteractiveCMD('rclone config')
    cmd.start(verbose=verbose)
    cmd.inputs(['n',conn_name, '27', '', '', '1', 'n', 'n', config_token, '', '1', 'y', 'q'], intervel=3, verbose=verbose)
    os.system('sudo nohup rclone --vfs-cache-mode writes mount %s: %s/%s >/var/log/rclone_onedrive.log 2>&1 &' % (conn_name, mount_prefix, conn_name))
    ONEDRIVE_PATH = '%s/%s' % (mount_prefix, conn_name)
    mkdir(ONEDRIVE_PATH)
    DATA_ROOT_PATH = os.path.join(ONEDRIVE_PATH, 'notebooks/data')


def main():
    pass


if __name__ == '__main__':
    main()