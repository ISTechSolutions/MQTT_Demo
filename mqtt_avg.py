#!/usr/bin/env python3

# PROGRAMMER: Jvdm
# DATE CREATED: 19/07/2019                                 
# REVISED DATE: 
# PURPOSE: MQTT Client 2 - Calc moving average of the incoming data for various time periods
#          * Publish averages every 10 seonds for 1 min, 5 min and 30 min averages

import paho.mqtt.client as mqtt
import random
import time
import json
import argparse

# default settings
broker_address = "iot.eclipse.org"
broker_port = 1883              
publish_topic   = "counter/avg"
subscribe_topic = "counter/val"

client_name = "avg_calc"        # MQTT client name

show_log   = False              # True : show log info
start_time = time.time()        # when the app was started

# create mqtt instance
client = mqtt.Client(client_name)

# Average Data dictionary 
avg_dict = {
  "avg_1min" : 0,
  "avg_5min" : 0,
  "avg_30min": 0
}

# Moving average time based FIFO
class AvgFifo:
    def __init__(self, period):
        self.first = None       # empty list
        self.last = None        # empty list
        self.avg = 0            # running average
        self.accum = 0          # Accumulator for avg
        self.count = 0          # # of samples in buffer
        self.period = period    # time period
        self.time = time.time() # start time

    def append(self, data):
        # [0:payload, 1:time, 2:'pointer']

        node = [data,time.time() - self.time, None]     
        if self.first is None:
            self.first = node   # append the node
        else:
            self.last[2] = node # append the node
        self.last = node

        self.accum += data      # add new value to accumulator
        self.count += 1         # one more sample
        #
        #print(self.first)
        #

    def pop(self):
        if self.first is None : 
            raise IndexError
        node = self.first       # get oldest node
        self.first = node[2]    # move 2nd oldest to oldest
        self.accum -= node[0]   # remove the value from accum
        self.count -= 1         # one less sample
        return node[0]          # return oldest

    def calc_average(self): 
        # if the time period expired, calc new avg
        now = time.time() - self.time
        # any data in the list to process?
        if self.first is not None :
            # did the oldest node expire?
            oldest_node_time = self.first[1]
            delta_time = now - oldest_node_time
            #
            #print("node time / now {:.1f} / {:.1f} Delta : {:.1f}".format(oldest_node_time,now,delta_time))
            #
            
            if delta_time > self.period :
                #
                #print("timer expired - removing oldest entry")
                #
                self.pop()

        if self.count > 0 :
            self.avg = self.accum / self.count
        else :    
            self.avg = 0
        #
        #print("Accum {} Count {} Avg {:.1f}".format(self.accum, self.count, self.avg))
        #
        return self.avg
    
    def get_avg(self):
        return self.avg

# Create the three average buffers with their time span
avg_fifio_1min = AvgFifo(60*1)
avg_fifio_5min = AvgFifo(60*5)
avg_fifio_30min = AvgFifo(60*30)

# process user arguments
def get_input_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pubtopic' , type=str, default=publish_topic, help='MQTT publish topic')
    parser.add_argument('--subtopic' , type=str, default=subscribe_topic, help='MQTT subscribe topic')
    parser.add_argument('--broker', type=str, default=broker_address, help='MQTT broker address')
    parser.add_argument('--port', type=str, default=broker_port, help='MQTT broker port')
    return parser.parse_args()

# publish averages, every specified time period
def publish_task(pub_period):
    pub_timer = 0
    while True:
        time.sleep(1)
        avg_dict["avg_1min"]  = avg_fifio_1min.calc_average()
        avg_dict["avg_5min"]  = avg_fifio_5min.calc_average()
        avg_dict["avg_30min"] = avg_fifio_30min.calc_average()
        pub_timer += 1

        # time to publish our data?
        if pub_timer >= pub_period :
            pub_timer = 0

            # convert into JSON:
            json_str = json.dumps(avg_dict)

            # publish averages as json
            print("Publish {} : {}".format(publish_topic, json_str))
            client.publish(publish_topic,json_str)

# sub receive callback 
def on_message(client, userdata, message):
    data_str = str(message.payload.decode("utf-8"))
    print("Received: {} = {} ".format(message.topic,data_str))
    avg_fifio_1min.append(int(message.payload))
    avg_fifio_5min.append(int(message.payload))
    avg_fifio_30min.append(int(message.payload))
    #print("retain :",message.retain)
    #print("qos    :",message.qos)

# disconnect
def on_disconnect(client, userdata,rc=0):
    logging.debug("Disconnected result code "+str(rc))
    client.loop_stop()

# log additional info (must be enabled)
def on_log(client, userdata, level, buf):
    print("log: ",buf)

# configure and connect
def mqtt_init() :
    global publish_topic
    global subscribe_topic
    global broker_address
    global broker_port

    # process cmd line arguments (if any)
    in_args = get_input_args()
    publish_topic  = in_args.pubtopic
    subscribe_topic= in_args.subtopic
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
        publish_task(10)
    except:
        print( "Exception encountered")
    finally:
        client.loop_stop()
        print( "Goodbye!")

#=============================================================================#
#                            EOF - mqtt_avg.py                                #
#=============================================================================#
