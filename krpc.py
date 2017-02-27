#!/usr/bin/env python
# coding=utf-8

import time
import random
import socket
import utility
import threading
from config import INITIAL_NODES
from bencode import bencode, bdecode
from dbconnect import save_info_hashs, save_rtable, save_get_peer_info_hashs

K = 8
NEW_K = 1500
TABLE_NUM = 160
TOKEN_LENGTH = 5
NODE_ID_LENGTH = 20
TRANS_ID_LENGTH = 2


class KRPC(object):
    def __init__(self, address):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.socket.bind(address)


class DHTProtocol(KRPC):
    def __init__(self, node_id, routing_table, address):
        KRPC.__init__(self, address)
        self.node_id = node_id
        self.routing_table = routing_table
        self.info_hash_list = []
        self.get_peer_info_hash_list = []

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

        try:
            self.socket.sendto(response, address)
        except:
            pass

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

        try:
            self.socket.sendto(response, address)
        except:
            pass

    def handle_gp_qdata(self, data, address):
        print "Receive get peer query"

        info_hash = data["a"]["info_hash"]
        self.get_peer_info_hash_list.append(info_hash)

        if len(self.get_peer_info_hash_list) >= 1000:
            save_get_peer_info_hashs(self.get_peer_info_hash_list)
            self.get_peer_info_hash_list = []

        response = bencode({
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id,
                "token": utility.generate_id(TOKEN_LENGTH),
                "nodes": utility.encode_nodes(self.get_k_closest_nodes(data["a"]["info_hash"]))
            }
        })

        try:
            self.socket.sendto(response, address)
        except:
            pass

    def handle_ap_qdata(self, data, address):
        print "(>_<)receive info_hash"

        info_hash = data["a"]["info_hash"]
        self.info_hash_list.append(info_hash)
        if len(self.info_hash_list) >= 100:
            save_info_hashs(self.info_hash_list)
            self.info_hash_list = []

        response = bencode({
            "t": data["t"],
            "y": "r",
            "r": {
                "id": self.node_id
            }
        })

        try:
            self.socket.sendto(response, address)
        except:
            pass

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
            data, address = self.socket.recvfrom(65536)
            self.handle(data, address)

    def client(self):
        if len(self.routing_table) == 0:
            nodes = []
            self.routing_table = [[] for i in range(TABLE_NUM)]

            for initial_node in INITIAL_NODES:
                nodes.append([utility.generate_node_id(), initial_node])

            nodes.append([self.node_id, self.socket.getsockname()])
            self.add_nodes_to_rtable(nodes)

        while True:
            self.find_nodes_using_rtable()
            self.save_rtable()

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

        try:
            self.socket.sendto(query, tuple(node[1]))
        except:
            pass

    def find_nodes_using_rtable(self):
        if self.rtable_mutex.acquire():
            try:
                for bucket in self.routing_table:
                    for node in bucket:
                        self.find_node(node)
            finally:
                self.rtable_mutex.release()

    def save_rtable(self):
        if self.rtable_mutex.acquire():
            try:
                save_rtable(self.node_id, self.routing_table, self.socket.getsockname())
            finally:
                self.rtable_mutex.release()

    def start(self):
        client_thread = threading.Thread(target=self.client)
        server_thread = threading.Thread(target=self.server)

        client_thread.start()
        server_thread.start()
