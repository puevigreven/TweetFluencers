import json
import logging
import os
import time

import nest_asyncio
import pandas as pd
import src.log_config as log_config

import twint

nest_asyncio.apply()

logging = log_config.get_logger()
logger = logging.getLogger(__name__)


class CollectRelevantUser:
    def __init__(self):
        self.data_folder_path = "../../data/raw/"
        self.ML_KEYWORDS = [
            "ai ",
            "ml ",
            ".ai" "fast.ai" "artificial intelligence",
            "machine learning",
            "deeplearning",
            "machinelearning",
            "nlproc",
            "nlp ",
            "computer vision",
            "computervision",
            "cv ",
            "reinforcement learning",
            "rl ",
            "kaggle",
            "datascience",
            "data science",
            "google brain",
            "deepmind",
            "googleai",
            "data scientist",
            "pattern analysis",
            "statistical modelling",
            "computational learning",
            "natural language processing",
            "vision and learning",
            "data visualization",
            "matplotlib",
            "computer science",
            "data ethics",
            "stats ",
            "deepmind",
            "intelligent systems",
            "a.i.",
            "pytorch",
            "tensorflow",
            "keras",
            "theano",
            "bayesian statistics",
            "openai",
            "forecasting",
        ]

    def scrape_follow_list(self, target):
        logger.info("===> " + str(target))
        c = twint.Config()
        c.Username = str(target)
        c.Hide_output = True
        c.Store_object = True
        twint.run.Following(c)
        follow_list = twint.output.follows_list
        return follow_list

    def write_to_file(self, relevant_user):
        if len(relevant_user) % 2 == 0:
            with open(
                self.data_folder_path + "refined_relevant_user_list.txt", "w+"
            ) as f:
                for item in relevant_user:
                    f.write("%s\n" % item)
            logger.info("====>   Data written to refined_relevant_user_list.txt file!")

    def write_user_info(self, relevant_user_info_list):
        if len(relevant_user_info_list) > 2:
            main_csv = pd.DataFrame()
            try:
                main_csv = pd.read_csv(self.data_folder_path + "relevant_user_info.csv")
            except Exception as e:
                logger.error(str(e))
                logger.error("Error occurred in write_user_info")
                print(relevant_user_info_list)
                relevant_user_info_df = pd.DataFrame.from_dict(relevant_user_info_list)
                relevant_user_info_df.to_csv(
                    self.data_folder_path + "relevant_user_info.csv", index=False
                )
                empty_list = []
                return empty_list

            relevant_user_info_df = pd.DataFrame(relevant_user_info_list)

            main_csv = main_csv.append(relevant_user_info_df)

            main_csv.to_csv(
                self.data_folder_path + "relevant_user_info.csv", index=False
            )
            logger.info("====>  Data Written to relevant_user_info.txt file!")
            empty_list = []
            return empty_list

        return relevant_user_info_list

    def is_relevant_user(self, user):
        twint.output.clean_lists()
        c = twint.Config()
        c.Username = str(user)
        c.Store_object = True
        c.Hide_output = True
        twint.run.Lookup(c)
        users_list = twint.output.users_list
        if len(users_list) == 0:
            return False, None
        user = users_list[0]
        user_info_dict = twint.storage.write_meta.userData(user)

        if any(word in str(user.bio).lower() for word in self.ML_KEYWORDS):
            return True, user_info_dict
        else:
            return False, None

    def main(self):
        last_stop_index = 0
        flag = True

        with open(self.data_folder_path + "refined_relevant_user_list.txt") as f:
            relevant_user = f.read().splitlines()
        if len(relevant_user) == 0 and flag:
            relevant_user = []
            relevant_user_info_list = []
            relevant_user.append("neuripsconf")
            completed_user = 0
        else:
            logger.info("Resuming from user number: " + str(last_stop_index))
            relevant_user_info_list = []
            relevant_user = relevant_user[last_stop_index:]
            completed_user = last_stop_index - 1

        last_count = len(relevant_user)

        for target in relevant_user:
            ts = time.time()
            follow_list = self.scrape_follow_list(target)

            for user in follow_list:
                try:
                    if user not in relevant_user:
                        rel, rel_user_info = self.is_relevant_user(user)
                        relevant_user_info_list.append(rel_user_info)
                        relevant_user_info_list = self.write_user_info(
                            relevant_user_info_list
                        )
                        if rel:
                            relevant_user.append(user)
                            self.write_to_file(relevant_user)
                except Exception as e:
                    logger.error(str(e))
                    logger.error("Error occurred with follow list user: " + str(user))
                    time.sleep(300)
                    continue

            completed_user = completed_user + 1
            logger.info("Number of people in follow list  : " + str(len(follow_list)))
            new_users_count = len(relevant_user) - last_count
            logger.info("Completed User                  : " + str(target))
            logger.info(
                "Took                            : "
                + str(time.time() - ts)
                + " seconds"
            )
            logger.info("Number of completed user        : " + str(completed_user))
            logger.info("Number of user in Relevant list : " + str(len(relevant_user)))
            logger.info("Number of new users added       : " + str(new_users_count))
            last_count = len(relevant_user)


if __name__ == "__main__":
    cru = CollectRelevantUser()
    cru.main()
