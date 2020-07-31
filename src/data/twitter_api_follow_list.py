import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pandas as pd
import src.log_config as log_config
import tweepy

logging = log_config.get_logger()
logger = logging.getLogger(__name__)


class TwitterAPIFollowList:
    def __init__(self):
        self.data_folder_path = "../../data/raw/"
        self.follow_list_dict = {}
        self.count = 0
        self.api = None

    def twitter_auth(self):
        access_token = os.environ.get("TWITTER_ACCESS_KEY")
        access_token_secret = os.environ.get("TWITTER_ACCESS_SECRET_KEY")
        consumer_key = os.environ.get("TWITTER_API_KEY")
        consumer_secret = os.environ.get("TWITTER_API_SECRET_KEY")

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True)

    def collect_follow_list(self):
        ts = time.time()
        follow_dict = {}
        # with open(self.data_folder_path + "relevant_user.txt") as f:
        #     relevant_user = f.read().splitlines()
        # relevant_user = relevant_user[:100]
        relevant_user = [
            # "JayAlammar",
            # "abhi1thakur",
            # "suzatweet",
            # "fchollet",
            # "A_K_Nain",
            # "NirantK",
            # "omarsar0",
            "lexfridman",
            # "bhutanisanyam1",
            # "drfeifei",
            # "PralayRamteke",
        ]
        for target_user in relevant_user:
            logger.info("Started for user: " + target_user)
            ids = []
            for page in tweepy.Cursor(
                self.api.friends, screen_name=target_user
            ).items():
                ids.append(page)
                time.sleep(60)

            self.follow_list_dict[str(target_user)] = ids

            self.count = self.count + 1
            logger.info(
                "completed: "
                + str(target_user)
                + " follow list count: "
                + str(len(follow_list))
                + " count: "
                + str(self.count)
            )
            # time.sleep(420)
        print(follow_list_dict)
        with open(
            self.data_folder_path + "result_follow_list_twit_api.json", "w+"
        ) as fp:
            json.dump(self.follow_list_dict, fp)
        logger.info("Took " + str(time.time() - ts))


if __name__ == "__main__":
    t_api = TwitterAPIFollowList()
    t_api.twitter_auth()
    t_api.collect_follow_list()
