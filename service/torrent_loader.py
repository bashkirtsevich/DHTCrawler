#!/usr/bin/env python
# coding=utf-8

import socket
import time
from utility import generate_node_id, from_hex_to_byte
from threading import Thread
from struct import unpack, pack
from bencode import bencode, bdecode, decode_dict


class TorrentLoader(object):
    def __init__(self, host, port, info_hash, on_metadata_loaded, on_finish):
        self.__host = host
        self.__port = port
        self.__info_hash = info_hash
        self.__on_metadata_loaded = on_metadata_loaded
        self.__on_finish = on_finish

        self.__metadata_size = 0
        self.__metadata = {}
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __connect(self):
        self.__socket.connect((self.__host, self.__port))

    def __disconnect(self):
        self.__socket.close()

    def __send(self, message_data):
        self.__socket.send(message_data)

    def __receive(self, size):
        if size > 0:
            return self.__socket.recv(size)
        else:
            return None

    def __send_handshake(self):
        self.__send((
            chr(19) +
            "BitTorrent protocol" +
            from_hex_to_byte("0000000000100005") +
            self.__info_hash +
            generate_node_id()
        ))

    def __send_extended_handshake(self):
        msg_data = bencode({
            "v": "DHT Crawler",
            "e": 0,
            "m": {"ut_metadata": 1},
            "reqq": 255
        })
        self.__send((
            pack("!I", len(msg_data) + 2) +
            chr(20) +
            chr(0) +
            msg_data
        ))

    def __send_metadata_request(self, extension_id, piece):
        msg_data = bencode({"msg_type": 0, "piece": piece})
        self.__send((
            pack("!I", len(msg_data) + 2) +
            chr(20) +
            chr(extension_id) +
            msg_data
        ))

    def __read_handshake(self):
        self.__receive(68)

    def __read_message(self):
        msg_len = unpack("!I", self.__receive(4))[0]

        if msg_len >= 1:
            return unpack("B", self.__receive(1))[0], self.__receive(msg_len - 1)
        else:
            return None

    def __load(self):
        self.__connect()
        try:
            # Send handshake message
            self.__send_handshake()

            # Wait for response data
            self.__read_handshake()

            time.sleep(0.1)
            self.__send_extended_handshake()

            # Read next message
            while True:
                msg = self.__read_message()

                if msg is not None:
                    msg_id, msg_data = msg

                    # Ignore all other messages except "extended"
                    if msg_id != 20:
                        continue

                    def piece_iterator(metadata_size):
                        return range(0, 1 + metadata_size / (16 * 1024))

                    e_msg_id = unpack("B", msg_data[0])[0]

                    if e_msg_id == 0:
                        extensions = bdecode(msg_data[1:])

                        if "m" in extensions and "ut_metadata" in extensions["m"] and "metadata_size" in extensions:
                            time.sleep(0.1)

                            self.__metadata_size = extensions["metadata_size"]
                            ut_metadata_id = extensions["m"]["ut_metadata"]

                            for i in piece_iterator(self.__metadata_size):
                                self.__send_metadata_request(ut_metadata_id, i)

                    elif e_msg_id == 1:
                        response = msg_data[1:]
                        r_dict, r_len = decode_dict(response, 0)
                        self.__metadata[r_dict["piece"]] = response[r_len:]

                        metadata = reduce(lambda a, e: a + self.__metadata[e],
                                          piece_iterator(self.__metadata_size), "")

                        if len(metadata) == self.__metadata_size and self.__on_metadata_loaded is not None:
                            self.__on_metadata_loaded(metadata)

                            return
        finally:
            self.__disconnect()

            if self.__on_finish is not None:
                self.__on_finish()

    def start(self):
        load_thread = Thread(target=self.__load)
        load_thread.start()
