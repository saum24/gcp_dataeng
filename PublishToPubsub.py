#!/usr/bin/env python
# coding: utf-8

# In[23]:


import os
import requests
import json
import pandas as pd
from time import sleep
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
import logging

# In[3]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "amiable-anagram-322816-9453c1d45c91.json"
crypto_url = "https://api.nomics.com/v1/currencies/ticker"
crypto_config = {"tickers": "BTC,ETH,DOGE", "currency": "USD"}


# In[19]:


class PublishToPubsub:
    def __init__(self):
        self.ProjectId = 'amiable-anagram-322816'
        self.TopicId = 'Cap1_Topic1'
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.ProjectId, self.TopicId)
        self.publish_futures = []

    def crypto_data(self) -> str:

        params = {
            "key": "cd2e479b10898934cda305d1fc74f5b608763ba3",
            # "key": os.environ.get("Api_key", ""),
            "ids": crypto_config['tickers'],
            "convert": crypto_config['currency'],
            'interval': "1d",
            "per-page": "100",
            "page": "1"
        }

        session = requests.Session()
        rest_req = session.get(crypto_url, params=params, stream=True)

        if 200 <= rest_req.status_code < 400:
            logging.info(f"Response - {rest_req.status_code}:{rest_req.text}")
            return rest_req.text
        else:
            raise Exception(f"Failed to fetch API data - {rest_req.status_code}:{rest_req.text}")

    def get_callback(self, publish_future: Future, data: str) -> callable:
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")

        return callback

    def PublishToTopic(self, message: str) -> None:
        publish_future = self.publisher_client.publish(self.topic_path, message.encode('utf-8'))
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info("Published messages with error handler to {self.topic_path}.")


# In[ ]:


if __name__ == "__main__":
    topic_publish = PublishToPubsub()
    for i in range(15):
        message = topic_publish.crypto_data()
        topic_publish.PublishToTopic(message)
        sleep(30)

