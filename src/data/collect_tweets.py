import logging
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from time import time

import src.log_config as log_config

import twint

logging = log_config.get_logger()
logger = logging.getLogger(__name__)


class CollectTweets:
    def download_tweet(self, user_name):
        ts = time()
        logger.info("Started ===> " + user_name)

        c = twint.Config()
        c.Username = user_name
        c.Hide_output = True
        c.Since = "2019-01-01"
        c.Limit = 2000
        c.Store_csv = True
        c.Min_wait_time = 120
        data_folder_path = "../../data/raw/tweet_since_2019/"
        c.Output = data_folder_path + user_name + ".csv"
        twint.run.Search(c)

        logger.info("Completed ===> " + user_name)
        logger.info("One user time took %s", time() - ts)

    def main(self):
        data_folder_path = "../../data/raw/"
        relevant_user_file = data_folder_path + "relevant_user.txt"
        with open(relevant_user_file) as f:
            user_list = f.read().splitlines()
        print(len(user_list))
        ts = time()
        with ProcessPoolExecutor() as executor:
            executor.map(self.download_tweet, user_list)
        logger.info("Total time Took %s", time() - ts)


if __name__ == "__main__":
    ct = CollectTweets()
    ct.main()
