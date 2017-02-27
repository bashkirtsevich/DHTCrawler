#!/usr/bin/env python
# coding=utf-8

import config
import pymongo
import utility
import datetime
from node import Node


def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    try:
        database = client.dhtcrawler

        def get_routing_tables():
            routing_tables_collection = database.rtables

            routing_tables = list(routing_tables_collection.find())

            for rtable in routing_tables:
                rtable["node_id"] = utility.from_hex_to_byte(rtable["node_id"])
                for bucket in rtable["rtable"]:
                    for node in bucket:
                        node[0] = utility.from_hex_to_byte(node[0])

            return routing_tables

        def save_routing_table(node_id, routing_table, address):
            routing_tables_collection = database.rtables

            node_id = utility.from_byte_to_hex(node_id)
            for bucket in routing_table:
                for node in bucket:
                    node[0] = utility.from_byte_to_hex(node[0])

            if routing_tables_collection.find_one({"node_id": node_id}):
                routing_tables_collection.update({"node_id": node_id}, {"$set": {"rtable": routing_table}})
            else:
                rtable_record = {
                    "node_id": node_id,
                    "addr": list(address),
                    "rtable": routing_table
                }
                routing_tables_collection.insert(rtable_record)

            for bucket in routing_table:
                for node in bucket:
                    node[0] = utility.from_hex_to_byte(node[0])

        def save_info_hashes(info_hash, host, port, announce_port):
            info_hashs_collection = database.info_hashs

            info_hash_record = {
                "value": utility.from_byte_to_hex(info_hash),
                "host": host,
                "port": port,
                "announce_port": announce_port,
                "date": datetime.datetime.utcnow()
            }
            info_hashs_collection.insert(info_hash_record)

        def save_get_peer_info_hashes(info_hash):
            get_peer_info_hashes_collection = database.get_peer_info_hashs

            get_peer_info_hash_record = {
                "value": utility.from_byte_to_hex(info_hash),
                "date": datetime.datetime.utcnow()
            }
            get_peer_info_hashes_collection.insert(get_peer_info_hash_record)

        node_num = config.NODE_NUM
        nodes = [None for i in range(node_num)]

        rtables = get_routing_tables()

        for i in range(min(node_num, len(rtables))):
            nodes[i] = Node(rtables[i]["node_id"], rtables[i]["rtable"], tuple(rtables[i]["addr"]),
                            on_get_peers=save_get_peer_info_hashes,
                            on_announce=save_info_hashes,
                            on_save_routing_table=save_routing_table
                            )
            nodes[i].protocol.start()

        for i in range(len(rtables), node_num):
            nodes[i] = Node(address=("0.0.0.0", 12346),
                            on_get_peers=save_get_peer_info_hashes,
                            on_announce=save_info_hashes,
                            on_save_routing_table=save_routing_table
                            )
            nodes[i].protocol.start()
    finally:
        client.close()


if __name__ == '__main__':
    main()
