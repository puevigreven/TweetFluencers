import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial

import nest_asyncio
import pandas as pd
import src.log_config as log_config

import twint

nest_asyncio.apply()


logging = log_config.get_logger()
logger = logging.getLogger(__name__)


class CollectFollowList:
    def __init__(self):
        self.data_folder_path = "../../data/raw/"
        self.follow_list_dict = {}
        self.count = 0

    def get_follow_list(self, target_user):
        try:

            twint.output.clean_lists()
            logger.info("started: " + str(target_user))
            # global self.follow_list_dict
            # global self.count
            c = twint.Config()
            c.Username = str(target_user)
            c.Hide_output = True
            c.Store_object = True
            c.Min_wait_time = 120
            twint.run.Following(c)
            follow_list = twint.output.follows_list
            self.follow_list_dict[str(target_user)] = follow_list
            twint.output.clean_lists()
            self.count = self.count + 1
            logger.info(
                "completed: "
                + str(target_user)
                + " follow list count: "
                + str(len(follow_list))
                + " count: "
                + str(self.count)
            )
        except Exception as e:
            logger.info(str(e))
            logger.info("Error occurred with follow list user: " + str(user))
            time.sleep(300)

    def process_main(self):
        # global self.follow_list_dict
        ts = time.time()
        with open(self.data_folder_path + "relevant_user.txt") as f:
            user_list = f.read().splitlines()
        user_list = user_list[:10]
        logger.info(len(user_list))
        with ProcessPoolExecutor() as executor:
            executor.map(self.get_follow_list, user_list)

        logger.info("Done")
        with open(self.data_folder_path + "result_follow_list.json", "w+") as fp:
            json.dump(self.follow_list_dict, fp)
        logger.info("Took " + str(time.time() - ts))

    def thread_main(self):
        # global self.follow_list_dict
        ts = time.time()
        with open(self.data_folder_path + "relevant_user.txt") as f:
            user_list = f.read().splitlines()
        user_list = user_list[:5]
        logger.info(len(user_list))
        with ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(self.get_follow_list, user_list)

        logger.info("Done")
        with open(self.data_folder_path + "result_follow_list.json", "w+") as fp:
            json.dump(self.follow_list_dict, fp)
        logger.info("Took " + str(time.time() - ts))


if __name__ == "__main__":
    cfl = CollectFollowList()
    cfl.thread_main()
