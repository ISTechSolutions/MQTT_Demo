#!/usr/bin/env python3

# PROGRAMMER: Jvdm
# DATE CREATED: 19/07/2019                                 
# REVISED DATE: 
# PURPOSE: MQTT Client 3 - Print Averages

import paho.mqtt.client as mqtt
import time
import json
import argparse

# default settings
broker_address = "iot.eclipse.org"
broker_port = 1883              # port to connect to (default)
subscribe_topic = "counter/avg"
client_name = "avg_prnt"       # MQTT client name

show_log   = False              # True : show log info
start_time = time.time()        # timekeeping

# create mqtt instance
client = mqtt.Client(client_name)

# a Python object (dict):
avg_dict = {
  "avg_1min" : 0,
  "avg_5min" : 0,
  "avg_30min": 0
}

def get_input_args():
    # Create argument parser
    parser = argparse.ArgumentParser()
    # Add command line arguments
    parser.add_argument('--topic' , type=str, default=subscribe_topic, help='MQTT subscribe topic')
    parser.add_argument('--broker', type=str, default=broker_address, help='MQTT broker address')
    parser.add_argument('--port', type=str, default=broker_port, help='MQTT broker port')
    return parser.parse_args()

# receive callback 
def on_message(client, userdata, message):
    global avg_dict 
    global start_time
    delta_time = time.time()-start_time
    start_time = time.time()

    json_str = str(message.payload.decode("utf-8"))
    #
    print("Received: {} = {} time {:.1f}".format(message.topic,json_str,delta_time))
    #

    # update the dictionary
    avg_dict = json.loads(json_str)

    # print result
    print(" 1 minute average : {:.1f}".format( avg_dict["avg_1min"] ) )
    print(" 5 minute average : {:.1f}".format( avg_dict["avg_5min"] ) )
    print("30 minute average : {:.1f}\n".format( avg_dict["avg_30min"] ) )

# disconnect
def on_disconnect(client, userdata,rc=0):
    logging.debug("Disconnected result code "+str(rc))
    client.loop_stop()

# log additional info (must be enabled)
def on_log(client, userdata, level, buf):
    print("log: ",buf)

# configure and connect
def mqtt_init() :
    global subscribe_topic
    global broker_address
    global broker_port

    # process cmd line arguments (if any)
    in_args = get_input_args()

    subscribe_topic  = in_args.topic
    broker_address = in_args.broker
    broker_port    = int(in_args.port)

    # register callbacks:
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    if show_log == True :
        client.on_log = on_log

    # initiate connection
    print("Connecting to broker : {}:{} ".format(broker_address, broker_port))
    client.connect(broker_address,broker_port) 
    # process pub/sub messages
    client.loop_start() 

    # subscibe to our topic
    print("Subscribing to topic :",subscribe_topic)
    client.subscribe(subscribe_topic)

if __name__ == "__main__":
    try:
        mqtt_init()
        while True:
            time.sleep(1)

    except:
        print( "Exception encountered")
    finally:
        client.loop_stop()
        print( "Goodbye!")

#=============================================================================#
#                            EOF - mqtt_prnt.py                               #
#=============================================================================#
