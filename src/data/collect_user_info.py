import json
import logging
import os
import time

import nest_asyncio
import pandas as pd
import src.log_config as log_config
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import pandas as pd 
import twint

nest_asyncio.apply()

logging = log_config.get_logger()
logger = logging.getLogger(__name__)

class CollectUserInfo():
    def __init__(self):
        self.all_users_info_dict = []
        self.data_folder_path = "../../data/raw/"
        
        
    def get_user_info(self, username):
        c = twint.Config()
        c.Username = str(username)
        c.Store_object = True
        c.Hide_output = True
        twint.run.Lookup(c)
        users = twint.output.users_list[-1]
        user_info_dict = twint.storage.write_meta.userData(users)
        # print(user_info_dict)
        self.all_users_info_dict.append(user_info_dict)
        if len(self.all_users_info_dict) % 100 == 0:
            logger.info("Collected " + str(len(self.all_users_info_dict)) + " user information!")
        
    def main(self):
        ts = time.time()
        with open(self.data_folder_path + "relevant_user.txt") as f:
            relevant_user = f.read().splitlines()    
        
        # relevant_user = relevant_user[:200]
        logger.info(len(relevant_user))
        with ThreadPoolExecutor(max_workers=100) as executor:
            executor.map(self.get_user_info, relevant_user)

        logger.info("Done")
        df = pd.DataFrame(self.all_users_info_dict)
        df.to_csv(self.data_folder_path + "relevant_user_info.csv",index= False)
        logger.info("Took " + str(time.time() - ts))
        
        
        
if __name__ == "__main__":
    cui = CollectUserInfo()
    cui.main()