import os
import glob
import pandas as pd
from tqdm import tqdm


import collections
import re
import sys
import time
from src.utils.mongodb_helper import mongodb_func


class TopNWords:
    def __init__(self):
        super().__init__()

    def tokenize(self, string):
        """Convert string to lowercase and split into words (ignoring
        punctuation), returning list of words.
        """
        return re.findall(r"\w+", string.lower())

    def count_ngrams(self, lines, min_length=1, max_length=3):
        """Iterate through given lines iterator (file object or list of
        lines) and return n-gram frequencies. The return value is a dict
        mapping the length of the n-gram to a collections.Counter
        object of n-gram tuple and number of times that n-gram occurred.
        Returned dict includes n-grams of length min_length to max_length.
        """
        lengths = range(min_length, max_length + 1)
        ngrams = {length: collections.Counter() for length in lengths}
        queue = collections.deque(maxlen=max_length)

        # Helper function to add n-grams at start of current queue to dict
        def add_queue():
            current = tuple(queue)
            for length in lengths:
                if len(current) >= length:
                    ngrams[length][current[:length]] += 1

        # Loop through all lines and words and add n-grams to dict
        for line in lines:
            for word in self.tokenize(line):
                queue.append(word)
                if len(queue) >= max_length:
                    add_queue()

        # Make sure we get the n-grams at the tail end of the queue
        while len(queue) > min_length:
            queue.popleft()
            add_queue()

        return ngrams

    def print_most_frequent(self, ngrams, num=10):
        """Print num most common n-grams of each length in n-grams dict."""
        keywords = []
        for n in sorted(ngrams):
            # print('----- {} most common {}-grams -----'.format(num, n))
            for gram, count in ngrams[n].most_common(num):
                # print('{0}: {1}'.format(' '.join(gram), count))
                keywords.append(" ".join(gram))
                # print(gram)
            # print('')
        return keywords


if __name__ == "__main__":
    mongo_helper = mongodb_func()
    extension = "txt"
    all_filenames = [
        i for i in glob.glob("../../data/interim/cleaned_tweets/*.{}".format(extension))
    ]

    top_n_words = TopNWords()
    start_time = time.time()
    for file in tqdm(all_filenames):
        base = os.path.basename(file)
        username = os.path.splitext(base)[0]
        # print (username)
        top_n_words = TopNWords()
        with open(file) as f:
            ngrams = top_n_words.count_ngrams(f)
        keywords = top_n_words.print_most_frequent(ngrams)
        mongo_helper.find_and_update(username, keywords)
    elapsed_time = time.time() - start_time
    print("Took {:.03f} seconds".format(elapsed_time))
