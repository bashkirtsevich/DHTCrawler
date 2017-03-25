#!/usr/bin/env python
# coding=utf-8

from flask import Flask
from flask import render_template
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client.dhtcrawler


@app.route("/")
def show_index():
    info_hashes = db.info_hashes.distinct("value")

    return render_template("index.html", info_hashes=info_hashes)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
