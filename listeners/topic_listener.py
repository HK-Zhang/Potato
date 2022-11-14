import os
import logging
import json
import sys

from azure.servicebus import ServiceBusClient, AutoLockRenewer

import azure_secrets as secrets

class TopicListener:
    def __init__(self, listener_name, topic_name, subscription_name):
        self.listener_name = listener_name
        self.topic_name = topic_name
        self.subscription_name = subscription_name

    def listen(self):
        pid = os.getpid()
        logging.info(f'{pid} says:{self.listener_name} start working.')
        self.__monitor_topic_loop()

    def __monitor_topic_loop(self):
        connstr = secrets.get_value('SERVICE_BUS_CONNECTION_STR')
        pid = os.getpid()
        with AutoLockRenewer(max_workers=4) as renewer:
            renewer.renew_period = 120
            with ServiceBusClient.from_connection_string(connstr) as client:
                    # while True:
                    with client.get_subscription_receiver(
                            self.topic_name, self.subscription_name) as receiver:
                        def on_lock_renew_failure_callback(renewable, error):
                            logging.warning(f'{os.getpid()} says:intentionally failed to renew lock on {renewable} due to {error}')
                        for message in receiver:
                            renewer.register(receiver,
                                                message,
                                                max_lock_renewal_duration=4 * 3600,
                                                on_lock_renew_failure=on_lock_renew_failure_callback)
                            if message.application_properties[
                                    b"Encrypted"] == False:
                                payload = json.loads(str(message))
                            else:
                                # TODO
                                payload = json.loads(str(message))
                            logging.info(
                                f'{pid} says:receive message - {payload["Id"]}')
                            self.process_message(payload)
                            try:
                                receiver.complete_message(message)
                                logging.info(
                                    f'{pid} says:complete message - {payload["Id"]}'
                                )
                            except:
                                logging.info(
                                    f'{pid} says:failed message - {payload["Id"]}')
                                logging.exception(sys.exc_info())
            renewer.close()

    def process_message(self, message_body):
        pid = os.getpid()