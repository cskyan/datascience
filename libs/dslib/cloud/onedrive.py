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
DATA_ROOT_PATH = os.path.join(ONEDRIVE_PATH, 'notebooks/data')


os.system('sudo apt -qq install fuse -y')


def mkdir(path, verbose=False):
  if path and not os.path.exists(path):
    if verbose: print(("Creating folder: " + path))
    os.makedirs(path)


def fixdir(path, default_reldir='.', verbose=False):
    try:
        mkdir(path, verbose=verbose)
        if not os.access(path, os.W_OK): raise PermissionError('Cannot access `%s` !' % path)
        return os.path.abspath(path)
    except Exception as e:
        if verbose: print(e)
        default_dir = os.path.join('.', default_reldir)
        mkdir(default_dir, verbose=verbose)
        return os.path.abspath(default_dir)


def download(cache_dir='.cache', pkg_versions={}, ignore_cache=False, verbose=False):
    cache_dir = fixdir(cache_dir, default_reldir='.cache', verbose=verbose)
    if ignore_cache or not os.path.exists(os.path.join(cache_dir, 'rclone-v%s-linux-amd64.deb' % pkg_versions.setdefault('rclone', RCLONE_VERSION))):
        os.system('wget %s https://downloads.rclone.org/v%s/rclone-v%s-linux-amd64.deb -P %s' % ('' if verbose else '-q', pkg_versions.setdefault('rclone', RCLONE_VERSION), pkg_versions.setdefault('rclone', RCLONE_VERSION), cache_dir))
    os.system('sudo apt %s install %s' % ('' if verbose else '-qq', os.path.join(cache_dir, 'rclone-v%s-linux-amd64.deb' % pkg_versions.setdefault('rclone', RCLONE_VERSION))))


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


class CloudShell(object):
    def __init__(self, cloud_path_root, local_path_root, verbose=False):
        self.cloud_path_root = os.path.abspath(cloud_path_root)
        self.local_path_root = os.path.abspath(local_path_root)
        self.verbose = verbose

    def sync(self, fpath):
        local_fpath = os.path.abspath(fpath)
        if not local_fpath.startswith(self.local_path_root):
            print('The file [%s] is not mapped to cloud location [%s]!' % (fpath, self.cloud_path_root))
            return False
        relative_fpath = os.path.relpath(local_fpath, self.local_path_root)
        cloud_fpath = os.path.join(self.cloud_path_root, relative_fpath)
        if os.path.exists(local_fpath):
            if not os.path.exists(cloud_fpath) or ( os.path.exists(cloud_fpath) and os.path.getmtime(cloud_fpath) < os.path.getmtime(local_fpath)):
                mkdir(os.path.dirname(cloud_fpath), verbose=self.verbose)
                import shutil
                shutil.copy(local_fpath, cloud_fpath)
        else:
            if os.path.exists(cloud_fpath):
                mkdir(os.path.dirname(local_fpath), verbose=self.verbose)
                import shutil
                shutil.copy(cloud_fpath, local_fpath)
            else:
                print('Cannot find file [%s] on the cloud!' % cloud_fpath)
                return False
        return True

    def batch_sync(self, fpaths):
        for fpath in fpaths: self.sync(fpath)

    def open(self, fpath, *args, **kwargs):
        if not self.sync(fpath): return None
        return open(fpath, *args, **kwargs)

    def read_csv(self, fpath, *args, **kwargs):
        if not self.sync(fpath): return None
        import pandas
        return pandas.read_csv(fpath, *args, **kwargs)

    def read_excel(self, fpath, *args, **kwargs):
        if not self.sync(fpath): return None
        import pandas
        return pandas.read_excel(fpath, *args, **kwargs)


def mount(prefix=None, conn=None, token=None, log_dir='/var/log', remount=True, verbose=False):
    log_dir = fixdir(log_dir, default_reldir='log', verbose=verbose)
    mount_prefix = input('Please input the mount location [default: /content]:').rstrip('/') or '/content' if prefix is None else str(prefix)
    conn_name = input('Please input a connection name [default: %s]:' % DEFAULT_CONN_NAME) or DEFAULT_CONN_NAME if conn is None else str(conn)
    config_token = input('Please input your config token: \nHint: You may get it by executing `rclone authorize "onedrive"` \n') if token is None else str(token)
    if remount or subprocess.check_output('CONN=`rclone config show | grep -w "\[%s\]"`; if [ -z "$CONN" ]; then echo false; else echo true; fi' % conn_name, shell=True).decode('utf-8').strip('\n') == 'false':
        os.system('rclone config delete %s' % conn_name)
        cmd = InteractiveCMD('rclone config')
        cmd.start(verbose=verbose)
        cmd.inputs(['n',conn_name, '27', '', '', '1', 'n', 'n', config_token, '', '1', 'y', 'q'], intervel=3, verbose=verbose)
        os.system('nohup rclone --allow-non-empty --vfs-cache-mode writes mount %s: %s/%s >%s/rclone_onedrive.log 2>&1 &' % (conn_name, mount_prefix, conn_name, log_dir))
        if verbose: print('Check rclone log in %s/rclone_onedrive.log' % log_dir)
    ONEDRIVE_PATH = '%s/%s' % (mount_prefix, conn_name)
    mkdir(ONEDRIVE_PATH, verbose=verbose)
    DATA_ROOT_PATH = os.path.join(ONEDRIVE_PATH, 'notebooks/data')


def main():
    pass


if __name__ == '__main__':
    main()
