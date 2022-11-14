import os
import logging
import threading as td
import time
import json
import sys

from azure.servicebus import ServiceBusClient, AutoLockRenewer

import secrets
from config import queue_name
from utilities import CloudStorageClient


# set a unique name of the listener
listener_name = "message_listener"


def listen():
    pid = os.getpid()
    logging.info(f'{pid} says:{listener_name} start working.')
    listener_thread = td.Thread(target=__monitor_queue_loop)
    listener_thread.daemon = True
    listener_thread.setName(listener_name)
    listener_thread.start()

    gurad_thread = td.Thread(target=__guard_thread)
    gurad_thread.daemon = True
    gurad_thread.setName(f'{listener_name}_guard')
    gurad_thread.start()


def __guard_thread(sleeptimes=180):
    while True:
        nowThreadsName = []
        now = td.enumerate()
        pid = os.getpid()
        for i in now:
            nowThreadsName.append(i.getName())

        if listener_name in nowThreadsName:
            logging.info(f'{pid} says:{listener_name} is running.')
        else:
            logging.info(f'{pid} says:{listener_name} stopped.')
            listener_thread = td.Thread(target=__monitor_queue_loop)
            listener_thread.setName(listener_name)
            listener_thread.start()
        time.sleep(sleeptimes)


def __monitor_queue_loop():
    connstr = secrets.get_value('SERVICE_BUS_CONNECTION_STR')
    pid = os.getpid()

    with ServiceBusClient.from_connection_string(connstr) as client:
        with AutoLockRenewer(max_workers=4) as renewer:
            renewer.renew_period = 120
            while True:
                with client.get_queue_receiver(queue_name, max_wait_time=30) as receiver:
                    for message in receiver:
                        def on_lock_renew_failure_callback(renewable, error):
                            logging.warning(f'{os.getpid()} says:intentionally failed to renew lock on {renewable} due to {error}')
                        renewer.register(receiver, message,
                                         max_lock_renewal_duration=4 * 3600,on_lock_renew_failure=on_lock_renew_failure_callback)
                        if message.application_properties["Encrypted"] == False:
                            payload = json.loads(str(message))
                        else:
                            # TODO
                            payload = json.loads(str(message))
                        logging.info(
                            f'{pid} says:receive message - {payload["Id"]}')
                        __process_message(payload)
                        try:
                            receiver.complete_message(message)
                            logging.info(
                                f'{pid} says:complete message - {payload["Id"]}')
                        except:
                            logging.info(
                                f'{pid} says:failed message - {payload["Id"]}')
                            logging.exception(sys.exc_info())


def __process_message(message_body):
    pass
