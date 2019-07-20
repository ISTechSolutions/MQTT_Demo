#!/usr/bin/env python3

# PROGRAMMER: Jvdm
# DATE CREATED: 19/07/2019                                 
# REVISED DATE: 
# PURPOSE: MQTT Client
#          * Publish a random number on average every 30 second
#          * Debug : Subscribe to same publish topic and print the result

import paho.mqtt.client as mqtt
import random
import time
import argparse

# default settings
# broker "192.168.1.184" or "iot.eclipse.org"
#        "test.mosquitto.org" or "broker.hivemq.com"
broker_address = "iot.eclipse.org"
broker_port    = 1883        
publish_topic  = "counter/val"
client_name    = "rnd_gen"  # MQTT client name

time_frame = 30             # average time between publishing msg
max_tx_val = 100            # value between 1 and 100
show_log   = False          # True : show log info
start_time = time.time()    # when the app was started
pub_time   = 0.0            # msg round trip time

# random number generators (transmit value, wait time)
# PURPOSE: Creates a pseudo random number mot exceeding max_val
class MyRandom:
    # Init this instance with the max value
    def __init__(self,max_val):
        self.max_val = max_val

    # generate a pseudo random number within range
    def value(self):
        return int(random.randint(0,self.max_val))

# random number instances 
rnd_tx_wait = MyRandom(time_frame)
rnd_tx_val  = MyRandom(max_tx_val)

# create mqtt instance
client = mqtt.Client(client_name)

def get_input_args():
    # Create argument parser
    parser = argparse.ArgumentParser()
    # Add command line arguments
    parser.add_argument('--topic' , type=str, default=publish_topic, help='MQTT publish topic')
    parser.add_argument('--broker', type=str, default=broker_address, help='MQTT broker address')
    parser.add_argument('--port', type=str, default=broker_port, help='MQTT broker port')
    return parser.parse_args()

# publish our topic on average at 30 seconds
def publish_task(period,f,*args):
    while True:
        # time to wait before we publish
        wait_time = rnd_tx_wait.value()
        print("Wait", wait_time)
        time.sleep(wait_time)
        # publish our topic
        f(*args)
        
# receive callback 
def on_message(client, userdata, message):
    delta_time = time.time() - pub_time
    print("Received: {} = {}, trip time: {:.2f}s".format(message.topic,str(message.payload.decode("utf-8")),delta_time))
    #print("retain :",message.retain)
    #print("qos    :",message.qos)

# disconnect
def on_disconnect(client, userdata,rc=0):
    logging.debug("DisConnected result code "+str(rc))
    client.loop_stop()

# log additional info (must be enabled)
def on_log(client, userdata, level, buf):
    print("log: ",buf)

# publish a topic and value
def mqtt_publish(topic):
    global pub_time
    rnd_val = rnd_tx_val.value()
    print('Publish : {} = {}, run time ({:.1f})'.format(topic,rnd_val,time.time()-start_time))
    client.publish(publish_topic,rnd_val)
    # updeate tx time to calc round trip time
    pub_time = time.time()

# configure and connect
def mqtt_init() :
    global publish_topic
    global broker_address
    global broker_port

    # process cmd line arguments (if any)
    in_args = get_input_args()

    publish_topic  = in_args.topic
    broker_address = in_args.broker
    broker_port    = int(in_args.port)

    # callbacks:
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
    print("Subscribing to topic :",publish_topic)
    client.subscribe(publish_topic)

if __name__ == "__main__":
    try:
        mqtt_init()
        publish_task(time_frame,mqtt_publish,publish_topic)
    except:
        print( "Exception encountered")
    finally:
        client.loop_stop()
        print( "Goodbye!")

#=============================================================================#
#                            EOF - mqtt_rnd.py                                #
#=============================================================================#
