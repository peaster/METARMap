import paho.mqtt.client as mqtt
import mqtt_config as config
import os
from metarmap import MetarMap
import light_utils

broker_url = config.broker_url
broker_port = config.broker_port
light_state = "OFF"
brightness_state = 0

map = MetarMap()

def on_connect(client, userdata, flags, rc):
   print("Connected With Result Cqode: {}".format(rc))

def on_command_topic_message(client, userdata, message):
    decodedMessage = message.payload.decode()
    print("Command topic message received from broker: "+ decodedMessage)
    if decodedMessage == 'ON':
        map.updateLights(brightness_state)
        light_state = "ON"
    elif (decodedMessage == 'OFF') :
        map.shutdownLights()
        light_state = "OFF"

def on_brightness_topic_message(client, userdata, message):
    decodedMessage = message.payload.decode()
    print("Brightness topic message received from broker: "+ decodedMessage)
    if (decodedMessage == 0) :
        map.shutdownLights()
        light_state = "OFF"
        brightness_state = decodedMessage
    else :
        map.updateLights(light_utils.convert_brightness_to_percentage(decodedMessage))
        brightness_state = decodedMessage
        light_state = "ON"

client = mqtt.Client('metarmap')
client.on_connect = on_connect
#To Process Every Other Message
client.on_message = on_message

if config.broker_username:
    client.username_pw_set(username=config.broker_username, password=config.broker_password)
client.connect(broker_url, broker_port)

# Command Topic Subscription
client.subscribe(config.command_topic, qos=1)
client.message_callback_add(config.command_topic, on_command_topic_message)

# Brightness Topic Subscription
client.subscribe(config.brightness_set_topic, qos=1)
client.message_callback_add(config.command_topic, on_command_topic_message)

# Topic Publishers
client.publish(topic=config.state_topic, payload=light_state, qos=0, retain=True)
client.publish(topic=config.brightness_state_topic, payload=brightness_state, qos=0, retain=True)

client.loop_forever()