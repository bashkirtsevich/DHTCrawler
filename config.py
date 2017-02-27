#!/usr/bin/env python
# coding=utf-8

# DHTCrawler config
NODE_NUM = 1

INITIAL_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
]

# database config
MONGODB_HOST = "127.0.0.1"
MONGODB_PORT = 27017
