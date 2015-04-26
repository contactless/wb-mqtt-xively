# coding:utf-8
import sys

import mosquitto
import argparse
import xively

api = None
feed = None

MQTT_META_UNITS = {
    'temperature'         : '°C',
    'rel_humidity'        : '%, RH',
    'atmospheric_pressure'        : 'millibar',
    'rainfall'        : 'mm/hour',
    'wind_speed'        : 'm/s',
    'power'        : 'watt',
    'power_consumption'        : 'kWh',
    'voltage'        : 'volts',
    'water_flow'        : 'm^3 / hour',
    'water_consumption'        : 'm^3',
    'resistance'        : 'Ohm',
}

from collections import defaultdict

class Control(object):
    def __init__(self):
        self.meta = {}
        self.stream = None

controls_meta = defaultdict( Control)


def find_stream(device_id, control_id):
    for stream in feed.datastreams.list():

        mqtt_device_id = None
        mqtt_control_id = None

        if stream._data['tags']:
            for tag in stream._data['tags']:
                tag_parts = tag.split('=', 1)
                if len(tag_parts) == 2:
                    if tag_parts[0] == 'mqtt_device_id':
                        mqtt_device_id = tag_parts[1]
                    elif tag_parts[0] == 'mqtt_control_id':
                        mqtt_control_id = tag_parts[1]

        if mqtt_device_id == device_id and mqtt_control_id == control_id:
            return stream

    return None

def create_stream(device_id, control_id):
    stream_id = "%s_%s" % (device_id, control_id)
    stream_id = stream_id.replace(' ','_')
    stream = feed.datastreams.create(stream_id)
    if stream._data['tags'] is None:
        stream._data['tags'] = []
    stream._data['tags'].append("mqtt_device_id=%s" % device_id)
    stream._data['tags'].append("mqtt_control_id=%s" % control_id)
    stream.update()
    return stream



def on_mqtt_message(arg0, arg1, arg2=None):
    if arg2 is None:
        mosq, obj, msg = None, arg0, arg1
    else:
        mosq, obj, msg = arg0, arg1, arg2


    print msg.topic

    parts = msg.topic.split('/')
    if parts[1] != 'devices':
        return
    if parts[3] != 'controls':
        return

    device_id = parts[2]
    control_id = parts[4]


    control_desc = controls_meta[(device_id, control_id)]
    if control_desc.stream is None:
        control_desc.stream = find_stream(device_id, control_id)

        if control_desc.stream is None:
            control_desc.stream = create_stream(device_id, control_id)



    if len(parts) > 6:
        if parts[5] == 'meta':
            meta = parts[6]
            control_desc.meta[meta] = msg.payload

            units = None
            if meta == 'type':
                meta_type = msg.payload
                unit = None

                units = MQTT_META_UNITS.get(msg.payload)




            if meta == 'type' or meta == 'units':
                # meta/type = 'value'  and  meta/units

                if control_desc.meta.get('type') == 'value':
                    units = control_desc.meta.get('units')



            if units:
                control_desc.stream._data['unit'] = xively.Unit(symbol=units)
                control_desc.stream.update()


    if len(parts) == 5:
        control_desc.stream.current_value = msg.payload
        control_desc.stream.update(fields=['current_value'])








def main():
    global feed
    parser = argparse.ArgumentParser(description='MQTT-Xively bridge', add_help=False)

    parser.add_argument('-h', '--host', dest='host', type=str,
                     help='MQTT host', default='localhost')

    parser.add_argument('-p', '--port', dest='port', type=int,
                     help='MQTT port', default='1883')

    parser.add_argument('api_key' ,  type=str,
                     help='Xively api key')

    parser.add_argument('feed_id' ,  type=int,
                     help='Xively feed id')

    args = parser.parse_args()


    api = xively.XivelyAPIClient(args.api_key)
    feed = api.feeds.get(args.feed_id)

    client = mosquitto.Mosquitto()
    client.connect(args.host, args.port)
    client.on_message = on_mqtt_message

    client.subscribe('/devices/+/controls/+')
    client.subscribe('/devices/+/controls/+/meta/+')




    while 1:
        rc = client.loop()
        if rc != 0:
            break




if __name__ == '__main__':
    main()
