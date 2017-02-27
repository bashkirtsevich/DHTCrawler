#!/usr/bin/env python
# coding=utf-8

import utility
import datetime
import pymongo
from config import MONGODB_HOST, MONGODB_PORT


def save_info_hashes(info_hashes):
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    try:
        database = client.dhtcrawler
        info_hashs_collection = database.info_hashs

        for info_hash in info_hashes:
            info_hash_record = {
                "value": utility.from_byte_to_hex(info_hash),
                "date": datetime.datetime.utcnow()
            }
            info_hashs_collection.insert(info_hash_record)
    finally:
        client.close()


def get_info_hashes():
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    try:
        database = client.dhtcrawler
        info_hashs_collection = database.info_hashs

        info_hashs = []
        for info_hash_record in info_hashs_collection.find():
            info_hash_record["value"] = utility.from_hex_to_byte(info_hash_record["value"])
            info_hashs.append(info_hash_record)

    finally:
        client.close()

    return info_hashs


def get_gp_info_hashes():
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    try:
        database = client.dhtcrawler
        gp_info_hashes_collection = database.get_peer_info_hashs

        gp_info_hashes = list(gp_info_hashes_collection.find())
    finally:
        client.close()

    return len(gp_info_hashes)


def save_get_peer_info_hashes(get_peer_info_hashes):
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    try:
        database = client.dhtcrawler
        get_peer_info_hashes_collection = database.get_peer_info_hashs

        for get_peer_info_hash in get_peer_info_hashes:
            get_peer_info_hash_record = {
                "value": utility.from_byte_to_hex(get_peer_info_hash),
                "date": datetime.datetime.utcnow()
            }
            get_peer_info_hashes_collection.insert(get_peer_info_hash_record)
    finally:
        client.close()


def save_routing_table(node_id, routing_table, address):
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    try:
        database = client.dhtcrawler
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
    finally:
        client.close()


def get_routing_tables():
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    database = client.dhtcrawler
    routing_tables_collection = database.rtables

    routing_tables = list(routing_tables_collection.find())

    for rtable in routing_tables:
        rtable["node_id"] = utility.from_hex_to_byte(rtable["node_id"])
        for bucket in rtable["rtable"]:
            for node in bucket:
                node[0] = utility.from_hex_to_byte(node[0])

    client.close()

    return routing_tables


def save_bt_info(bt_info):
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    database = client.dhtcrawler
    bt_info_collection = database.bt_infos

    try:
        bt_info_collection.insert(bt_info)
    except:
        print "Cannot insert bt_info into database"

    client.close()


def get_bt_info():
    client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
    database = client.dhtcrawler
    bt_info_collection = database.bt_infos

    return list(bt_info_collection.find())
