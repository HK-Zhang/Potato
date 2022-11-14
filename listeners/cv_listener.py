import os
import logging
import sys

from .topic_listener import TopicListener
from utilities import VirtualFileSystemClient
import json


class CVListener(TopicListener):
    def __init__(self, listener_name, topic_name, subscription_name):
        super(CVListener, self).__init__(
            listener_name, topic_name, subscription_name)

    def process_message(self, message_body):
        pid = os.getpid()
        logging.info(f'{pid} says: start download data.')
        resourceClient = VirtualFileSystemClient(sas=message_body["ResourceSas"])
        statusClient = VirtualFileSystemClient(sas=message_body["StatusSas"])
        outputClient = VirtualFileSystemClient(sas=message_body["OutputSas"])
        local_file = resourceClient.download()
        logging.info(f'{pid} says: complete download data.')
        payload = message_body["Payload"]

        try:
            result_dir="result"
            logging.info(f'{pid} says: start upload data.')
            outputClient.upload(result_dir)
            statusClient.save("success")
            logging.info(f'{pid} says: complete upload data.')
        except:
            logging.info(f'{pid} says: failed message - {message_body["Id"]}')
            logging.exception(sys.exc_info())
            statusClient.save("failed")

