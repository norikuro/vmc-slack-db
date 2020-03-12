import pymongo
import datetime
#import logging

from vmcjp.utils import constant

#logger = logging.getLogger()
#logger.setLevel(logging.INFO)

def get_client(url):
    return pymongo.MongoClient(url)

def get_event_db(url):
    return get_client(url)[constant.USER_DB]

def get_cred_db(url):
    return get_client(url)[constant.CRED_DB]

def get_event_collection(url):
    return get_event_db(url)[constant.USER_COLLECTION]

def get_cred_collection(url):
    return get_cred_db(url)[constant.CRED_COLLECTION]

#def _read_event_db(url, user_id, minutes=None):
def read_event_db(url, user_id, minutes=None):
    event_col =get_event_collection(url)

    if minutes is None:
        data = event_col.find_one({"_id": user_id})
    else:
        past = (
            datetime.datetime.now() - datetime.timedelta(minutes=minutes)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        data = event_col.find_one({"start_time": {"$gt": past}, "_id": user_id})
        
    return data

#def read_event_db(event):
#    return _read_event_db(event.get("db_url"), event.get("user_id"), event.get("minutes"))
    
#def _read_cred_db(url, user_id):
def read_cred_db(url, user_id):
    cred_col = get_cred_collection(url)
    return cred_col.find_one({"_id": user_id})

#def read_cred_db(event):
#    return _read_cred_db(event.get("db_url"), event.get("user_id"))

#def _write_event_db(url, user_id, data):
def write_event_db(url, user_id, data):
    event_col = get_event_collection(url)
    now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    data.update({"start_time": now})
    event_col.update({"_id": user_id}, {"$set": data}, upsert=True)

#def write_event_db(event):
#    _write_event_db(event.get("db_url"), event.get("user_id"), event.get("data"))

#def _write_cred_db(url, user_id, data):
def write_cred_db(url, user_id, data):
    cred_col = get_cred_collection(url)
    cred_col.update({"_id": user_id}, {"$set": data}, upsert=True)

#def write_cred_db(event):
#    _write_cred_db(event.get("db_url"), event.get("user_id"), event.get("data"))

#def _delete_event_db(url, user_id):
def delete_event_db(url, user_id):
    event_col = get_event_collection(url)
    event_col.remove({"_id": user_id})

#def delete_event_db(event):
#    _delete_event_db(event.get("db_url"), event.get("user_id"))

#def _delete_cred_db(url, user_id):
def delete_cred_db(url, user_id):
    cred_col = get_cred_collection(url)
    cred_col.remove({"_id": user_id})

#def delete_cred_db(event):
#    _delete_cred_db(event.get("db_url"), event.get("user_id"))
    
def lambda_handler(event, context):
#    logging.info("user_id: {}".format(event.get("user_id")))
    return eval(event.get("db_command"))(event)
