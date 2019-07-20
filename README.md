# MQTT_Demo
Demo applications to showcase collaboration between a couple of MQTT clients

There are three independant python scripts, each responsible for doing a bit of work
  * **mqtt_rnd.py** - Generates random values within a specified range and publish them randomly within a 30 seconds time period
  * **mqtt_avg.py** - Subscribes to the above topic and calculates the averages for 1,5 and 30 minutes, using a moving average filter
                  It publishes the results in JSON format every 10 seconds
  * **mqtt_prnt.py** - Subscribes to the above topic and display the three results
  
## Notes:
* Parameters can be passed to the script to overide the default values such as :
    - Broker address
    - Broker port
    - Pub / Sub topic name
* By default the scripts will use the public MQTT **"iot.eclipse.org"** broker 
  
  
