import sys
import time
import random
import datetime
import serial . tools . list_ports
import pytz
import pymongo
from Adafruit_IO import MQTTClient
from Adafruit_IO import Client

# from getFeedData import 
client = pymongo.MongoClient(
    'mongodb+srv://peteoz6903:06092003@cluster0.jgl7eab.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
)
db = client["multidisciplinary"]
collection1 = db['fan']
collection2 = db['ledrgb']
collection3 = db['fanauto']
collection5 = db['temp']
collection6 = db['humid']
collection7 = db['ledrgbauto']
# collection7 = db['assistant']
# collection8 = db["thieftcontrol"]
# collection10 = db["activity"]
AIO_FEEDS = ["ledrgb", "temp", "humid", "fan", "fanauto", "ledrgbauto"]

AIO_USERNAME = "hungpham1406"
AIO_KEY = "aio_kxwQ606qvHocChiIHwc6u5mBZrRx"
aio = Client(AIO_USERNAME, AIO_KEY)


def connected(client):
    print("Connected successfully...")
    for feed in AIO_FEEDS:
        client.subscribe(feed)
        print(f"Subscribed to feed: {feed}")

def subscribe(client, userdata, mid, granted_qos):
    print("Subscribed successfully...")

def disconnected(client):
    print("Disconnected...")
    sys.exit(1)

def message(client , feed_id , payload):
    if(feed_id == "temp" or feed_id == "ledrgb" or feed_id == "humid" or feed_id == "light" or feed_id == "fan" or feed_id == "fanauto" or feed_id == "ledrgbauto"):
        print("Nhan du lieu: " + payload + " , feed_id: " + feed_id, "created_at: " + str(datetime.datetime.utcnow()) )
        if feed_id == "temp":
            collection5.insert_one({"feed_id": feed_id, "value": payload, "created_at": datetime.datetime.utcnow()})
        if feed_id == "humid":
            collection6.insert_one({"feed_id": feed_id, "value": payload, "created_at": datetime.datetime.utcnow()})
            # collection1.insert_one({"feed_id": feed_id, "value": payload, "created_at": datetime.datetime.utcnow()})
        if feed_id == "fan":
            collection1.insert_one({"feed_id": feed_id, "value": payload,"created_at": datetime.datetime.utcnow()})
        if feed_id == "ledrgb":
            collection2.insert_one({"feed_id": feed_id, "value": payload,"created_at": datetime.datetime.utcnow()})
        if feed_id == "fanauto":
            collection3.insert_one({"feed_id": feed_id, "value": payload,"created_at": datetime.datetime.utcnow()})
        if feed_id == "ledrgbauto":
            collection7.insert_one({"feed_id": feed_id, "value": payload,"created_at": datetime.datetime.utcnow()})



def safe_convert_to_float(value):
    try:
        # Try converting to float
        return float(value)
    except ValueError:
        # Return original string if conversion fails
        return value

def fetch_sensor_data(feed_key):
    data = aio.receive(feed_key)
    data_structure = {
        "id": data.id,
        "value": safe_convert_to_float(data.value),
        "feed_id": data.feed_id,
        "feed_key": feed_key,
        "created_at":  datetime.datetime.utcnow(),
    }
    print(data_structure)
    if "fan" in data_structure["feed_key"]:
        collection1.insert_one(data_structure)

    if "ledrgb" in data_structure["feed_key"]:
        collection2.insert_one(data_structure)
    if "fanauto" in data_structure["feed_key"]:
        collection3.insert_one(data_structure)
    if "temp" in data_structure["feed_key"]:
        collection5.insert_one(data_structure)
    if "humid" in data_structure["feed_key"]:
        collection6.insert_one(data_structure)
    # if "assistant" in data_structure["feed_key"]:
    #     collection7.insert_one(data_structure)

client = MQTTClient(AIO_USERNAME, AIO_KEY)
client.on_connect = connected
client.on_disconnect = disconnected
client.on_message = message
client.on_subscribe = subscribe
client.connect()
client.loop_background()
counter = 10
sensorType = 0

while True:
    pass

# # from getFeedData import *
# import pymongo 

# AIO_FEED_IDs = ["temp","light","humid", "fan","ledrgb","fanauto"]
# AIO_USERNAME = "hungpham1406"
# AIO_KEY = "aio_kxwQ606qvHocChiIHwc6u5mBZrRx"
# FEED_KEYS = ['smarthome.tempsensor', 'smarthome.lightsensor','smarthome.humisensor']  

# def connected(client):
#     print("Ket noi thanh cong ...")
#     for topic in AIO_FEED_IDs:
#         client.subscribe(topic)
#         print("Connecting to topic " + topic)

# def subscribe(client , userdata , mid , granted_qos):

#     print("Subscribe thanh cong ...")
#     for feed in AIO_FEED_IDs:
#         client.subscribe(feed)
#         print(f"Subscribed to feed: {feed}")

# def disconnected(client):
#     print("Ngat ket noi ...")
#     sys.exit (1)

    

# client = MQTTClient(AIO_USERNAME , AIO_KEY)
# client.on_connect = connected
# client.on_disconnect = disconnected
# client.on_message = message
# client.on_subscribe = subscribe
# client.connect()
# client.loop_background()
# counter = 10
# sensorType = 0 #send sensor's data to server sequentially, use SWITCH CASE
# while True:
#     # counter = counter -1
#     # if counter <= 0:
#     #     #send data to server with 10s interval
#     #     print("Random data is published to server")
#     #     counter = 10
#     #     temp = random.randint(10,40)
#     #     client.publish("sensor1"  ,temp)
#     #     light = random.randint(100,500)
#     #     client.publish("sensor2"  ,light)
    
#     # time.sleep(1)
#     # for feed_key in FEED_KEYS:
#     #         fetch_sensor_data(feed_key)
#     time.sleep(10)
#     pass

# List of Feed IDs to subscribe to
