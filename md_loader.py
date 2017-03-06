#!/usr/bin/env python
# coding=utf-8

import socket
from utility import generate_node_id
from threading import Thread


class MDLoader(object):
    def __init__(self, host, port, info_hash):
        self.__host = host
        self.__port = port
        self.__info_hash = info_hash

        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __connect(self):
        self.__socket.connect((self.__host, self.__port))

    def __disconnect(self):
        self.__socket.close()

    def __send(self, message_data):
        self.__socket.send(message_data)

    def __receive(self, size):
        return self.__socket.recv(size)

    def __create_handshake(self):
        # handshake: <pstrlen><pstr><reserved><info_hash><peer_id>
        return (
            chr(19) +
            "BitTorrent protocol" +
            8 * chr(0) +
            self.__info_hash +
            generate_node_id()
        )

    def __load(self):
        self.__connect()
        try:
            # Send handshake message
            self.__send(self.__create_handshake())

            # Wait for response data
            hs_response = self.__receive(68)
        finally:
            self.__disconnect()

    def start(self):
        load_thread = Thread(target=self.__load)
        load_thread.start()
