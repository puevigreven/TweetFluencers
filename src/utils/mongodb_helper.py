from pymongo import MongoClient
import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient


class mongodb_func:
    def __init__(self):
        super().__init__()
        self.client = MongoClient()
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client["pymongo_test"]

    def find_and_update(self, username, keywords):
        self.db.user_info.find_one_and_update(
            {"username": username}, {"$set": {"keywords": keywords}}
        )

    def insert_user_info(self, filepath, delete_before= False):
        # CSV to JSON Conversion
        csvfile = open(filepath, "r")
        # csvfile = open('../../data/raw/rel_user_info.csv', 'r')
        reader = csv.DictReader(csvfile)
        if delete_before:
            self.db.user_info.drop()

        header = [
            "id",
            "name",
            "username",
            "bio",
            "location",
            "url",
            "join_date",
            "join_time",
            "tweets",
            "following",
            "followers",
            "likes",
            "media",
            "private",
            "verified",
            "profile_image_url",
            "background_image",
        ]

        for each in reader:
            row = {}
            for field in header:
                row[field] = each[field]

            self.db.user_info.insert(row)
