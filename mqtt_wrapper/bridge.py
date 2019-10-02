#!/usr/bin/python
import paho.mqtt.client as mqtt
import time
import traceback
import ssl

class bridge:

    def __init__(self, mqtt_topic = None, client_id = "bridge", user_id = None, password = None, host = "127.0.0.1", port = 8883, keepalive = 60, certificates={}, AWS_IoT=0):
        self.mqtt_topic = mqtt_topic
        self.client_id = client_id
        self.user_id = user_id
        self.password = password
        self.host = host
        self.port = port
        self.keepalive = keepalive

        self.disconnect_flag = False
        self.rc = 1
        self.timeout = 0

        self.client = mqtt.Client(self.client_id, clean_session = True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        if self.user_id and self.password:
            self.client.username_pw_set(self.user_id, self.password)
        elif certificates:
            self.port = 443
            ssl_context = self.__class__.ssl_alpn(certificates, AWS_IoT)
            self.client.tls_set_context(context=ssl_context)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_unsubscribe = self.on_unsubscribe
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish

        self.connect()

    @staticmethod
    def ssl_alpn(certificates, AWS_IoT):
        try:
            ssl_context = ssl.create_default_context()
            if(AWS_IoT):
                IoT_protocol_name = "x-amzn-mqtt-ca"
                ssl_context.set_alpn_protocols([IoT_protocol_name])
            ca = certificates['ca']
            cert = certificates['pem']
            private = certificates['key']
            #debug print opnessl version
            ssl_context.load_verify_locations(cafile=ca)
            ssl_context.load_cert_chain(certfile=cert, keyfile=private)

            return  ssl_context
        except Exception as e:
            print("exception ssl_alpn()")
            raise e

    def connect(self):
        while self.rc != 0:
            try:
                self.rc = self.client.connect(self.host, self.port, self.keepalive)
                self.client.loop_start()
            except Exception as e:
                print("connection failed")
            time.sleep(2)
            self.timeout = self.timeout + 2

    def msg_process(self, msg):
        pass

    def looping(self, loop_timeout = .1):
        self.client.loop(loop_timeout)

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        if self.mqtt_topic:
            self.client.subscribe(self.mqtt_topic)
        self.timeout = 0

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            if not self.disconnect_flag:
                print("Unexpected disconnection.")
                print("Trying reconnection")
                self.rc = rc
                self.connect()

    def on_message(self, client, userdata, msg):
        try:
            self.msg_process(msg)
        except Exception as e:
            print(traceback.format_exc())

    def unsubscribe(self):
        print(" unsubscribing")
        self.client.unsubscribe(self.mqtt_topic)

    def disconnect(self):
        print(" disconnecting")
        self.disconnect_flag = True
        self.client.disconnect()

    def on_unsubscribe(self, client, userdata, mid):
        if (self.mqtt_topic == '#'):
            print("Unsubscribed to all the topics" )
        else:
            print("Unsubscribed to '%s'" % self.mqtt_topic)

    def on_subscribe(self, client, userdata, mid, granted_qos):
        if (self.mqtt_topic == '#'):
            print("Subscribed to all the topics" )
        else:
            print("Subscribed to '%s'" % self.mqtt_topic)

    def on_publish(self, client, userdata, mid):
        pass

    def hook(self):
        self.unsubscribe()
        self.disconnect()
        print(" shutting down")

    def get_timeout(self):
        return self.timeout

    def publish(self, topic, payload = None, qos = 0, retain = False):
        self.client.publish(topic, payload, qos, retain)
