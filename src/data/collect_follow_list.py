import json
import logging
import os
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from os import listdir
from os.path import isfile, join

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
        self.follow_list_path = "../../data/raw/follow_lists"
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
            c.User_full = True

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

    def check_if_file_present(self, username):
        # logger.info ("check if file present")
        mypath = "../../data/raw/follow_lists/"
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        downloaded_user = []
        for i in onlyfiles:
            fname = i.split(".")[0]
            downloaded_user.append(fname)

        if username in downloaded_user:
            return
        raise

    def subprocess_cmd(self, command):
        try:
            logger.info("start of the subprocess")
            process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            self.proc_stdout = process.communicate()[0].strip()
            logger.info("end of the subprocess")
            logger.info(str(self.proc_stdout))
        except Exception as e:
            logger.error(str(e))
            logger.info("exception occered")

    def get_follow_list_with_retry(self, username):
        count = 0
        while count < 5:
            logger.info("attempt: " + str(count) + " for user: " + username)
            count = count + 1
            try:
                command = (
                    "cd ../../data/raw/follow_lists/; twint -u "
                    + username
                    + " --following -o "
                    + username
                    + ".txt --csv"
                )
                # logger.info(command)
                self.subprocess_cmd(command)

                self.check_if_file_present(username)
            except:
                logger.info("Sleeping for 1 min!")
                time.sleep(60)
                continue
            logger.info("Completed for user: " + username)
            break

    def multiprocess_follow_list_with_retry(self):
        onlyfiles = [
            f
            for f in listdir(self.follow_list_path)
            if isfile(join(self.follow_list_path, f))
        ]
        downloaded_user = []
        for i in onlyfiles:
            fname = i.split(".")[0]
            downloaded_user.append(fname)
        with open(self.data_folder_path + "refined_relevant_user_list.txt") as f:
            user_list = f.read().splitlines()
        logger.info("total users in refined list: " + str(len(user_list)))
        logger.info("downloaded users in refined list: " + str(len(downloaded_user)))
        user_list = list(set(user_list) - set(downloaded_user))
        logger.info("to be downloaded: " + str(len(user_list)))
        # user_list = user_list[:2]
        # for i in user_list:
        #     self.get_follow_list_with_retry(i)
        with ProcessPoolExecutor() as executor:
            executor.map(self.get_follow_list_with_retry, user_list)


if __name__ == "__main__":
    cfl = CollectFollowList()
    # cfl.thread_main()
    # list_of_user = [ "omarsar0", "lexfridman",  "bhutanisanyam1", "drfeifei",]

    cfl.multiprocess_follow_list_with_retry()
