import logging
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
import time

import src.log_config as log_config
from os import listdir
from os.path import isfile, join
import twint

logging = log_config.get_logger()
logger = logging.getLogger(__name__)


class CollectTweets:
    def download_tweet(self, user_name):
        ts = time.time()
        logger.info("Started ===> " + user_name)

        c = twint.Config()
        c.Username = user_name
        c.Hide_output = True
        # c.Since = "2019-01-01"
        c.Limit = 1500
        c.Store_csv = True
        c.Min_wait_time = 120
        data_folder_path = "../../data/raw/tweets_last_1500/"
        c.Output = data_folder_path + user_name + ".csv"
        twint.run.Search(c)

        logger.info("Completed ===> " + user_name)
        logger.info("One user time took " + str(time.time() - ts))

    def main(self):

        mypath = "../../data/raw/tweets_last_1500/"
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

        downloaded_user = []
        for i in onlyfiles:
            fname = i.split(".")[0]
            downloaded_user.append(fname)
        data_folder_path = "../../data/raw/"
        relevant_user_file = data_folder_path + "4k_rel_user.txt"
        with open(relevant_user_file) as f:
            user_list = f.read().splitlines()
        logger.info("user list: " + str(len(set(user_list))))
        logger.info("downloaded users list: " + str(len(set(downloaded_user))))
        user_list = set(user_list) - set(downloaded_user)
        logger.info("To be downloaded: " + str(len(user_list)))
        ts = time.time()
        with ProcessPoolExecutor() as executor:
            executor.map(self.download_tweet, user_list)
        logger.info("Total time Took " + str(time.time() - ts))


if __name__ == "__main__":
    for i in range(10):
        logger.info("iteration " + str(i))
        ct = CollectTweets()
        ct.main()
        logger.info("sleeping for 2 mins!")
        time.sleep(120)
