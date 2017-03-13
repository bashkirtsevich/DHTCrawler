#!/usr/bin/env python
# coding=utf-8

import pymongo
import utility
import datetime
from torrent_loader import TorrentLoader
from node import Node


def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    try:
        database = client.dhtcrawler

        loaders = {}

        def handle_ping_event():
            print "Receive ping"

        def handle_find_nodes_event():
            print "Find nodes"

        def get_routing_tables():
            routing_tables = list(database.routing_tables.find())

            for routing_table in routing_tables:
                routing_table["node_id"] = utility.from_hex_to_byte(routing_table["node_id"])
                for bucket in routing_table["routing_table"]:
                    for node in bucket:
                        node[0] = utility.from_hex_to_byte(node[0])

            return routing_tables

        def handle_save_routing_table(node_id, routing_table, address):
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

        def handle_announce_event(info_hash, host, announce_port):
            print "Announce hash", utility.from_byte_to_hex(info_hash), host, announce_port

            coll = database.info_hashes

            coll.insert({
                "value": utility.from_byte_to_hex(info_hash),
                "host": host,
                "port": announce_port,
                "date": datetime.datetime.utcnow()
            })

            torrents = database.torrents

            if not info_hash in loaders and not torrents.find_one({"info_hash": info_hash}):
                def save_metadata(metadata):
                    torrents.insert({
                        "info_hash": info_hash,
                        "metadata": metadata
                    })

                def release_loader():
                    del loaders[info_hash]

                loader = TorrentLoader(host, announce_port, info_hash,
                                       on_metadata_loaded=save_metadata,
                                       on_finish=release_loader)

                loaders[info_hash] = loader

                loader.start()

        def handle_get_peers_event(info_hash):
            print "Get peers", utility.from_byte_to_hex(info_hash)

            coll = database.get_peer_info_hashes

            coll.insert({
                "value": utility.from_byte_to_hex(info_hash),
                "timestamp": datetime.datetime.utcnow()
            })

        arguments = {
            "node_id": None,
            "routing_table": None,
            "address": ("0.0.0.0", 12346),
            "on_ping": handle_ping_event,
            "on_find_nodes": handle_find_nodes_event,
            "on_get_peers": handle_get_peers_event,
            "on_announce": handle_announce_event,
            "on_save_routing_table": handle_save_routing_table
        }

        routing_tables = get_routing_tables()

        if len(routing_tables) > 0:
            for routing_table in routing_tables:
                arguments["node_id"] = routing_table["node_id"]
                arguments["routing_table"] = routing_table["routing_table"]
                arguments["address"] = tuple(routing_table["address"])

                break

        nodes = []

        node = Node(**arguments)
        node.protocol.start()

        nodes.append(node)

    finally:
        client.close()


if __name__ == '__main__':
    main()
