import utils
from es_ops import *
from datetime import date, timedelta, datetime
import logging
import threading
import time

INDEX = "high-low-90"

data = utils.utils_data_refresh()


def update_data_source():
    data = utils.utils_data_refresh()
    for count, i in enumerate(data):
        while (threading.active_count() > 25):
            time.sleep(3)
        try:
            worker = threading.Thread(target=update_record, args=(i,))
            worker.start()
            print(threading.active_count())
            # worker.my_queue.get()
        except Exception as e:
            print(e)


def update_record(datapoint):
    print(datapoint)
    doc = {}
    doc["symbol"] = datapoint[0]
    doc["high"] = datapoint[1]
    doc["low"] = datapoint[2]
    doc["high_index"] = datapoint[3]
    doc["low_index"] = datapoint[4]
    es_update_document(INDEX, doc, datapoint[0])


if __name__ == "__main__":
    utils.utils_csv_refresh(delta=1)
    update_data_source()
