import sys
import logging
import socket
import signal
from opencensus.ext.azure.log_exporter import AzureLogHandler
from config import appversion, app_insights_key, topic_name
import azure_secrets as secrets
from listeners.cv_listener import CvListener


def init():
    secrets.init()
    hostname = socket.gethostname()
    format_str = f'{appversion}@{hostname} says:' + '%(asctime)s - %(levelname)-8s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(level=logging.INFO)
    formatter = logging.Formatter(format_str, date_format)
    rootlogger = logging.getLogger()
    handler = AzureLogHandler(connection_string=f'InstrumentationKey={app_insights_key}')
    handler.setFormatter(formatter)
    handler.setLevel(logging.ERROR)
    rootlogger.addHandler(handler)


def exit():
    sys.exit(0)


if __name__ == '__main__':
    try:
        init()
        signal.signal(signal.SIGINT, exit)
        signal.signal(signal.SIGTERM, exit)
        cv_listener = CvListener("CV_listener", topic_name, "CV")
        cv_listener.listen()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)