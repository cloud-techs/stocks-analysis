import utils
from es_ops import *
from datetime import date, timedelta, datetime
import logging
import threading
import time

INDEX = "52-week-high-low"


def update_data(i):
    year, month, day = utils.get_date_format(delta=1)
    doc  = {}
    comments = []
    index = "52-week-high-low"
    scrip = i["symbol"]
    #print(i)
    doc["stock"] = i["symbol"]
    doc["high"] = i["high"]
    doc["high_date"] = i["high_date"]
    doc["low"] = i["low"]
    doc["low_date"] = i["low_date"]
    curr_date = datetime.now()
    #print(type(curr_date))
    data = es_get_doc_source(INDEX, i["symbol"])
    #print("####", type(datetime.strptime(data["high_date"].split("T")[0], "%Y-%m-%d")))
   # print(data["high_date"].split("T")[0])
    prev_date = datetime.strptime(data["high_date"].split("T")[0], "%Y-%m-%d")
    prev_l = datetime.strptime(data["low_date"].split("T")[0], "%Y-%m-%d")
    diff_h =  (curr_date - datetime.strptime(data["high_date"].split("T")[0], "%Y-%m-%d")).days
    print(i["symbol"],diff_h)

    diff_l = (curr_date - datetime.strptime(data["low_date"].split("T")[0], "%Y-%m-%d")).days
    print(i["symbol"],diff_l)
    #print(scrip, i["high_date"], i["low_date"])

    ch = i["high_date"]
    cl = i["low_date"]


    if diff_h > 60:
        doc["updated-on"] = f"{year}-{month}-{day}"
        doc["comments"] = f"{scrip} made previous high on {prev_date} , updating latest high on {ch}"
        #print(f"{scrip} made previous high on {prev_l} , updating latest high on  {ch}")
    if diff_l > 60:
        doc["updated-on"] = f"{year}-{month}-{day}"
        doc["comments"] = f"{scrip} made previous low on {prev_date} , updating latest low on {cl}"
        #print(f"{scrip} made previous low on {prev_date} , updating latest low on {cl}")





