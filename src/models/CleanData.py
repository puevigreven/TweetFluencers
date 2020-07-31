import pandas as pd
import numpy as np

import nltk, sys
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import nltk.data
from nltk.stem.porter import *
import re
import os
import glob
import pandas as pd
import nltk
import csv


class CleanTweets:
    def __init__(self):
        super().__init__()
        self.my_stopwords = nltk.corpus.stopwords.words("english")
        # word_rooter = nltk.stem.snowball.PorterStemmer(ignore_stopwords=False).stem
        self.my_punctuation = "!\"$%&'()*+,-./:;<=>?[\\]^_`{|}~•@"
        self.csv_path = (
            "../../data/raw/tweets_last_1500"
        )
        self.cleaned_tweet_path = (
            "../../data/interim/"
        )

    def find_retweeted(self, tweet):
        """This function will extract the twitter handles of retweed people"""
        return re.findall("(?<=RT\s)(@[A-Za-z]+[A-Za-z0-9-_]+)", tweet)

    def find_mentioned(self, tweet):
        """This function will extract the twitter handles of people mentioned in the tweet"""
        return re.findall("(?<!RT\s)(@[A-Za-z]+[A-Za-z0-9-_]+)", tweet)

    def find_hashtags(self, tweet):
        """This function will extract hashtags"""
        return re.findall("(#[A-Za-z]+[A-Za-z0-9-_]+)", tweet)

    def remove_links(self, tweet):
        """Takes a string and removes web links from it"""
        tweet = re.sub(r"http\S+", "", tweet)  # remove http links
        tweet = re.sub(r"bit.ly/\S+", "", tweet)  # rempve bitly links
        tweet = re.sub(r"pictwittercom/\S+", "", tweet)  # rempve bitly links
        tweet = tweet.strip("[link]")  # remove [links]
        return tweet

    def remove_users(self, tweet):
        """Takes a string and removes retweet and @user information"""
        tweet = re.sub("(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)", "", tweet)  # remove retweet
        tweet = re.sub("(@[A-Za-z]+[A-Za-z0-9-_]+)", "", tweet)  # remove tweeted at
        return tweet

    # cleaning master function
    def clean_tweet(self, tweet, bigrams=True):

        tweet = self.remove_users(tweet)
        tweet = self.remove_links(tweet)

        tweet = tweet.lower()  # lower case
        tweet = re.sub(
            "[" + self.my_punctuation + "]+", " ", tweet
        )  # strip punctuation
        tweet = re.sub("\s+", " ", tweet)  # remove double spacing
        tweet = re.sub("([0-9]+)", "", tweet)  # remove numbers
        tweet_token_list = [
            word for word in tweet.split(" ") if word not in self.my_stopwords
        ]  # remove stopwords
        #     tweet_token_list = [word_rooter(word) if '#' not in word else word
        #                         for word in tweet_token_list] # apply word rooter
        #     if bigrams:
        #         tweet_token_list = tweet_token_list+[tweet_token_list[i]+'_'+tweet_token_list[i+1]
        #                                             for i in range(len(tweet_token_list)-1)]
        tweet = " ".join(tweet_token_list)
        return tweet

    def clean_and_save(self):

        os.chdir(self.csv_path)
        extension = "csv"
        all_filenames = [i for i in glob.glob("*.{}".format(extension))]

        for idx, i in enumerate(all_filenames):
            username = i.split(".")[0]
            try:
                try:
                    df = pd.read_csv(
                        i, engine="c", error_bad_lines=False, encoding="utf-8"
                    )
                except:
                    df = pd.read_csv(
                        i, engine="python", error_bad_lines=False, encoding="utf-8"
                    )
                finally:
                    print(int(idx) / len(all_filenames) * 100)
                    tweets = pd.DataFrame()
                    tweets["tweet_text_processed"] = df["tweet"]
                    # Load the regular expression library
                    # Remove punctuation
                    tweets["tweet_text_processed"] = tweets["tweet_text_processed"].map(
                        lambda x: re.sub("[,\.!?]", "", str(x))
                    )
                    # Convert the titles to lowercase
                    tweets["tweet_text_processed"] = tweets["tweet_text_processed"].map(
                        lambda x: x.lower()
                    )
                    # Print out the first rows of papers
                    tweets["tweet_text_processed"] = tweets.tweet_text_processed.apply(
                        self.clean_tweet
                    )
                    text_file = self.cleaned_tweet_path + username + ".txt"
                    tweets["tweet_text_processed"].to_csv(
                        text_file, header=False, index=False
                    )
            except Exception as e:
                continue


if __name__ == "__main__":
    ct = CleanTweets()
    ct.clean_and_save()
