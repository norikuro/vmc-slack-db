import pymongo
import datetime
import json

from vmcjp.utils import constant

def get_client(url):
    return pymongo.MongoClient(url)

def get_event_db(url):
    return get_client(url)[constant.USER_DB]

def get_cred_db(url):
    return get_client(url)[constant.CRED_DB]

def get_event_collection(url):
    return get_event_db(url)[constant.USER_COLLECTION]

def get_cred_collection(self):
    return get_cred_db(url)[constant.CRED_COLLECTION]

def read_event_db(user_id, minutes=None):
    event_col =get_event_collection(url)
    
    past = (
        datetime.datetime.now() - datetime.timedelta(minutes=minutes)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    if minutes is None:
        data = event_col.find_one({"_id": user_id})
    else:
        data = event_col.find_one({"start_time": {"$gt": past}, "_id": user_id})
    return data

def read_event_db(event):
    read_event_db(event.get(user_id), event.get("minutes"))

    def read_cred_db(self, user_id):
        data = self.cred_col.find_one({"_id": user_id})
        return data

    def write_event_db(self, user_id, data):
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        data.update({"start_time": now})
        self.event_col.update({"_id": user_id}, {"$set": data}, upsert=True)
        
    def write_cred_db(self, user_id, data):
        self.cred_col.update({"_id": user_id}, {"$set": data}, upsert=True)

    def delete_event_db(self, user_id):
        self.event_col.remove({"_id": user_id})
        
    def delete_cred_db(self, user_id):
        self.cred_col.remove({"_id": user_id})

    def init_sddc_db(self):
        sddc_db = self.client[constant.SDDC_DB]
        self.sddc_col = sddc_db[constant.SDDC_COLLECTION]

    def get_backedup_sddc_config(self):
        self.init_sddc_db()
        config = self.sddc_col.find(
            {}, 
            {
                "org.id": 1,
                "sddc_updated": 1,
                "sddc.id": 1,
                "sddc.name": 1,
                "sddc.region": 1,
                "sddc.num_hosts": 1,
                "sddc.vpc_cidr": 1,
                "org.display_name": 1,
                "customer_vpc.linked_account": 1,
                "customer_vpc.linked_vpc_subnets_id": 1,
                "aws_connected_account": 1,
                "_id": 0
            }
        )[0]
        
        ca = config.get("aws_connected_account")
        for a in ca:
            if config.get("customer_vpc").get("linked_account") == a.get("account_number"):
                a_id = a.get("id")
        
        return {
            "org_id": config["org"]["id"],
            "org_name": config["org"]["display_name"],
            "updated": config["sddc_updated"],
            "sddc_id": config["sddc"]["id"],
            "sddc_name": config["sddc"]["name"],
            "region": config["sddc"]["region"],
            "num_hosts": config["sddc"]["num_hosts"],
            "vpc_cidr": config["sddc"]["vpc_cidr"],
            "aws_account": config["customer_vpc"]["linked_account"],
            "customer_subnet_id": config["customer_vpc"]["linked_vpc_subnets_id"],
            "connected_account_id": a_id,
            "link_aws": "True"
        }
    
def lambda_handler(event, context):
    db = DocmentDb(event.get("db_url"))
    return eval(cmd)(event, db)
