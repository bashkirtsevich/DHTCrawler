#!/usr/bin/env python
# coding=utf-8

import time
import random
import socket
import utility
import threading
from config import INITIAL_NODES
from bencode import bencode, bdecode

K = 8
NEW_K = 1500
TABLE_NUM = 160
TOKEN_LENGTH = 5
NODE_ID_LENGTH = 20
TRANS_ID_LENGTH = 2


class KRPC(object):
    def __init__(self, address):
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.__socket.bind(address)

    def _send(self, data, address):
        try:
            self.__socket.sendto(data, address)
        except:
            pass

    def _receive(self):
        return self.__socket.recvfrom(65536)

    def _get_sock_name(self):
        return self.__socket.getsockname()


class DHTProtocol(KRPC):
    def __init__(self, node_id, routing_table, address, **kwargs):
        super(DHTProtocol, self).__init__(address)
        self.node_id = node_id
        self.routing_table = routing_table

        self._on_get_peers = kwargs["on_get_peers"] if "on_get_peers" in kwargs else None
        self._on_announce = kwargs["on_announce"] if "on_announce" in kwargs else None
        self._on_save_routing_table = kwargs["on_save_routing_table"] if "on_save_routing_table" in kwargs else None

        self.rtable_mutex = threading.Lock()

    def get_k_closest_nodes(self, id):
        rtable_index = utility.get_rtable_index(utility.xor(self.node_id, id))

        k_closest_nodes = []

        index = rtable_index
        while index >= 0 and len(k_closest_nodes) < K:
            for node in self.routing_table[index]:
                if len(k_closest_nodes) < K:
                    k_closest_nodes.append(node)
                else:
                    break
            index -= 1

        index = rtable_index + 1
        while index < 160 and len(k_closest_nodes) < K:
            for node in self.routing_table[index]:
                if len(k_closest_nodes) < K:
                    k_closest_nodes.append(node)
                else:
                    break
            index += 1

        return k_closest_nodes

    def add_nodes_to_rtable(self, nodes):
        if self.rtable_mutex.acquire():
            try:
                for node in nodes:
                    rtable_index = utility.get_rtable_index(utility.xor(node[0], self.node_id))
                    if len(self.routing_table[rtable_index]) < NEW_K:
                        self.routing_table[rtable_index].append(node)
                    else:
                        if random.randint(0, 1):
                            index = random.randint(0, NEW_K - 1)
                            self.routing_table[rtable_index][index] = node
                        else:
                            self.find_node(node)
            finally:
                self.rtable_mutex.release()

    def handle_pi_qdata(self, data, address):
        print "Receive ping query"

        response = bencode({
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id
            }
        })

        self._send(response, address)

    def handle_fn_qdata(self, data, address):
        print "Receive find node query"

        target_node_id = data["a"]["target"]
        rtable_index = utility.get_rtable_index(utility.xor(self.node_id, target_node_id))

        response_nodes = []
        for node in self.routing_table[rtable_index]:
            if node[0] == target_node_id:
                response_nodes.append(node)
                break

        if len(response_nodes) == 0:
            response_nodes = self.get_k_closest_nodes(target_node_id)

        node_message = utility.encode_nodes(response_nodes)

        response = bencode({
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "nodes": node_message
            }
        })

        self._send(response, address)

    def handle_gp_qdata(self, data, address):
        print "Receive get peer query"

        info_hash = data["a"]["info_hash"]

        host, port = address

        if self._on_get_peers is not None:
            self._on_get_peers(info_hash)

        response = bencode({
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "token": utility.generate_id(TOKEN_LENGTH),
                "nodes": utility.encode_nodes(self.get_k_closest_nodes(data["a"]["info_hash"]))
            }
        })

        self._send(response, address)

    def handle_ap_qdata(self, data, address):
        print "(>_<)receive info_hash"

        arguments = data["a"]
        info_hash = arguments["info_hash"]
        announce_port = arguments["port"]

        host, port = address

        if self._on_announce is not None:
            self._on_announce(info_hash, host, port, announce_port)

        response = bencode({
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id
            }
        })

        self._send(response, address)

    def handle_pi_rdata(self, data, address):
        pass

    def handle_fn_rdata(self, data, address):
        # print "Receive find node response"

        node_message = data["r"]["nodes"]
        nodes = utility.decode_nodes(node_message)
        self.add_nodes_to_rtable(nodes)

    def handle_gp_rdata(self, data, address):
        pass

    def handle_ap_rdata(self, data, address):
        pass

    def handle(self, data, address):
        try:
            data = bdecode(data)
        except:
            return

        query_handle_function = {
            "ping": self.handle_pi_qdata,
            "find_node": self.handle_fn_qdata,
            "get_peers": self.handle_gp_qdata,
            "announce_peer": self.handle_ap_qdata
        }

        try:
            type = data["y"]
            if type == "q":
                if data["q"] in query_handle_function.keys():
                    query_handle_function[data["q"]](data, address)
            elif type == "r":
                if data["r"].has_key("token"):
                    self.handle_gp_rdata(data, address)
                elif data["r"].has_key("nodes"):
                    self.handle_fn_rdata(data, address)
        except KeyError:
            pass

    def server(self):
        while True:
            data, address = self._receive()
            self.handle(data, address)

    def client(self):
        if len(self.routing_table) == 0:
            nodes = []
            self.routing_table = [[] for i in range(TABLE_NUM)]

            for initial_node in INITIAL_NODES:
                nodes.append([utility.generate_node_id(), initial_node])

            nodes.append([self.node_id, self._get_sock_name()])
            self.add_nodes_to_rtable(nodes)

        while True:
            self.find_nodes_using_routing_table()
            self.save_routing_table()

            time.sleep(4)

    def find_node(self, node):
        target_node_id = utility.generate_node_id()

        query = bencode({
            "t": utility.generate_id(TRANS_ID_LENGTH),
            "y": "q",
            "q": "find_node",
            "a": {
                "id": self.node_id,
                "target": target_node_id
            }
        })

        self._send(query, tuple(node[1]))

    def find_nodes_using_routing_table(self):
        if self.rtable_mutex.acquire():
            try:
                for bucket in self.routing_table:
                    for node in bucket:
                        self.find_node(node)
            finally:
                self.rtable_mutex.release()

    def save_routing_table(self):
        if self.rtable_mutex.acquire():
            try:
                if self._on_save_routing_table is not None:
                    self._on_save_routing_table(self.node_id, self.routing_table, self._get_sock_name())
            finally:
                self.rtable_mutex.release()

    def start(self):
        client_thread = threading.Thread(target=self.client)
        server_thread = threading.Thread(target=self.server)

        client_thread.start()
        server_thread.start()
