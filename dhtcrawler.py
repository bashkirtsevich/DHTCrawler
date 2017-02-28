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

        def print_ping_event():
            print "Receive ping"

        def print_find_nodes_event():
            print "Find nodes"

        def get_routing_tables():
            routing_tables = list(database.routing_tables.find())

            for routing_table in routing_tables:
                routing_table["node_id"] = utility.from_hex_to_byte(routing_table["node_id"])
                for bucket in routing_table["routing_table"]:
                    for node in bucket:
                        node[0] = utility.from_hex_to_byte(node[0])

            return routing_tables

        def save_routing_table(node_id, routing_table, address):
            coll = database.routing_tables

            node_id = utility.from_byte_to_hex(node_id)
            for bucket in routing_table:
                for node in bucket:
                    node[0] = utility.from_byte_to_hex(node[0])

            if coll.find_one({"node_id": node_id}):
                coll.update({"node_id": node_id}, {"$set": {"routing_table": routing_table}})
            else:
                coll.insert({
                    "node_id": node_id,
                    "address": list(address),
                    "routing_table": routing_table
                })

            for bucket in routing_table:
                for node in bucket:
                    node[0] = utility.from_hex_to_byte(node[0])

        def save_info_hashes(info_hash, host, port, announce_port):
            print "Announce hash", utility.from_byte_to_hex(info_hash), host, port, announce_port

            coll = database.info_hashes

            coll.insert({
                "value": utility.from_byte_to_hex(info_hash),
                "host": host,
                "port": port,
                "announce_port": announce_port,
                "date": datetime.datetime.utcnow()
            })

        def save_get_peer_info_hashes(info_hash):
            print "Get peers", utility.from_byte_to_hex(info_hash)

            coll = database.get_peer_info_hashes

            coll.insert({
                "value": utility.from_byte_to_hex(info_hash),
                "timestamp": datetime.datetime.utcnow()
            })

        node_num = config.NODE_NUM
        nodes = []

        routing_tables = get_routing_tables()

        for i in range(min(node_num, len(routing_tables))):
            node = Node(routing_tables[i]["node_id"], routing_tables[i]["routing_table"],
                        tuple(routing_tables[i]["address"]),
                        on_ping=print_ping_event,
                        on_find_nodes=print_find_nodes_event,
                        on_get_peers=save_get_peer_info_hashes,
                        on_announce=save_info_hashes,
                        on_save_routing_table=save_routing_table)

            node.protocol.start()
            nodes.append(node)

        for i in range(len(routing_tables), node_num):
            node = Node(address=("0.0.0.0", 12346),
                        on_ping=print_ping_event,
                        on_find_nodes=print_find_nodes_event,
                        on_get_peers=save_get_peer_info_hashes,
                        on_announce=save_info_hashes,
                        on_save_routing_table=save_routing_table)

            node.protocol.start()
            nodes.append(node)
    finally:
        client.close()


if __name__ == '__main__':
    main()
