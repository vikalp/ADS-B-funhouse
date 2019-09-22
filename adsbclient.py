#!/usr/bin/env python3
#
# Copyright (c) 2015 Johan Kanflo (github.com/kanflo)
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import socket, select
import argparse
import threading
import json
import sys
import logging
import remotelogger
import datetime, calendar
import signal
import random
import time
import re
import errno
import sbs1
import planedb
import mqtt_wrapper

log = logging.getLogger(__name__)

"""
A Python client listening to a dump1090 receiver and posting the MODE-S data
as JSON to an MQTT topic of your choice. If you have a running instance of
planedb-serv.py running, information about aircraft type, manufacturer,
registration and operator will be appended.
"""

# http://stackoverflow.com/questions/1165352/fast-comparison-between-two-python-dictionary
class DictDiffer(object):
  """
  Calculate the difference between two dictionaries as:
  (1) items added
  (2) items removed
  (3) keys same in both but changed values
  (4) keys same in both and unchanged values
  """
  def __init__(self, current_dict, past_dict):
    self.current_dict, self.past_dict = current_dict, past_dict
    self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
    self.intersect = self.set_current.intersection(self.set_past)
  def added(self):
    return self.set_current - self.intersect
  def removed(self):
    return self.set_past - self.intersect
  def changed(self):
    return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
  def unchanged(self):
    return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])

class Observation(object):
  def __init__(self, sbs1Message):
    log.debug("%s appeared" % sbs1Message.icao24)
    self.icao24 = sbs1Message.icao24
    self.loggedDate = sbs1Message.loggedDate
    self.callsign = sbs1Message.callsign
    self.altitude = sbs1Message.altitude
    self.groundSpeed = sbs1Message.groundSpeed
    self.track = sbs1Message.track
    self.lat = sbs1Message.lat
    self.lon = sbs1Message.lon
    self.verticalRate = sbs1Message.verticalRate
    self.operator = None
    self.registration = None
    self.type = None
    self.lost = False
    self.updated = True
    if args.pdb_host:
      plane = planedb.lookup_aircraft(self.icao24)
      if plane:
        self.registration = plane['registration']
        self.type = plane['manufacturer'] + " " + plane['model']
        self.operator = plane['operator']
      else:
        log.debug("icao24 %s not found in the database" % (self.icao24))

  def update(self, sbs1Message):
    self.loggedDate = sbs1Message.loggedDate
    oldData = dict(self.__dict__)
    if sbs1Message.icao24:
      self.icao24 = sbs1Message.icao24
    if sbs1Message.callsign and self.callsign != sbs1Message.callsign:
      self.callsign = sbs1Message.callsign
    if sbs1Message.altitude:
      self.altitude = sbs1Message.altitude
    if sbs1Message.groundSpeed:
      self.groundSpeed = sbs1Message.groundSpeed
    if sbs1Message.track:
      self.track = sbs1Message.track
    if sbs1Message.lat:
      self.lat = sbs1Message.lat
    if sbs1Message.lon:
      self.lon = sbs1Message.lon
    if sbs1Message.verticalRate:
      self.verticalRate = sbs1Message.verticalRate
    if not self.verticalRate:
      self.verticalRate = 0

    if args.pdb_host:
      plane = planedb.lookup_aircraft(self.icao24)
      if plane:
        self.registration = plane['registration']
        self.type = plane['manufacturer'] + " " + plane['model']
        self.operator = plane['operator']
      else:
        log.debug("icao24 %s not found in any data base" % (self.icao24))


    # Check if observation was updated
    newData = dict(self.__dict__)
    del oldData["loggedDate"]
    del newData["loggedDate"]
    d = DictDiffer(oldData, newData)
    self.updated = len(d.changed()) > 0


  def isPresentable(self):
    return self.altitude and self.groundSpeed and self.track and self.lat and self.lon


  def dump(self):
    log.debug("> %s %s - %s %s (%s), trk:%s spd:%s alt:%s %s, %s" % (self.icao24, self.callsign, self.operator, self.type, self.registration, self.track, self.groundSpeed, self.altitude, self.lat, self.lon))


  def dict(self):
    d =  dict(self.__dict__)
    if d["verticalRate"] == None:
      d["verticalRate"] = 0;
    if "lastAlt" in d:
      del d["lastAlt"]
    if "lastLat" in d:
      del d["lastLat"]
    if "lastLon" in d:
      del d["lastLon"]
    d["loggedDate"] = "%s" % (d["loggedDate"])
    return d

"""
Clean obsercations that are timeoutSec old. Post farewall ('lost = True') to
MQTT topic.
"""
def cleanObservations(observations, timeoutSec, bridge):
  global args
  removed = []
  now = datetime.datetime.now()
  for icao24 in observations:
    lastSeen = observations[icao24].loggedDate
    if lastSeen:
      lookDiff = now - lastSeen
      diffSeconds = (lookDiff.days * 86400 + lookDiff.seconds)
      if diffSeconds > timeoutSec:
        removed.append(icao24)

  for icao24 in removed:
    observations[icao24].lost = True
    observations[icao24].updated = True
    d = observations[icao24].dict()
    d["lost"] = True
    bridge.publish("/adsb/%s/json" % args.radar_name, json.dumps(d))
    del observations[icao24]
    log.debug("%s lost", icao24)

  return observations

def loggingInit(level, log_host):
  log = logging.getLogger(__name__)

  # Initialize remote logging
  logger = logging.getLogger()
  logger.setLevel(level)
  if log_host != None:
    remotelogger.init(logger = logger, appName = "adsbclient", subSystem = None, host = log_host, level = logging.DEBUG)

  # Log to stdout
  ch = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  logger.addHandler(ch)


def mqttThread(bridge):
  while True:
      bridge.looping()


def adsbThread(bridge):
  global args
  sock = None
  connWarn = False
  observations = {}
  socketTimeoutSec = 60
  cleanIntervalSec = 5
  cleanTimeoutSec = 30 # Clean observations when we have no updates in this time

  lastClean = datetime.datetime.utcnow()
  nextClean = datetime.datetime.utcnow() + datetime.timedelta(seconds=cleanIntervalSec)

  while True:
    if sock == None:
      try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((args.dump1090_host, args.dump1090_port))
        log.info("ADSB connected")
        sock.settimeout(socketTimeoutSec)
        connWarn = False
      except socket.error as e:
        if not connWarn:
          logging.critical("Failed to connect to ADSB receiver on %s:%s, retrying : %s" % (args.dump1090_host, args.dump1090_port, e))
          connWarn = True
        sock = None
        time.sleep(10)
    else:
      if datetime.datetime.utcnow() > nextClean:
        observations = cleanObservations(observations, cleanTimeoutSec, bridge)
        lastClean = datetime.datetime.utcnow()
        nextClean = datetime.datetime.utcnow() + datetime.timedelta(seconds=cleanIntervalSec)
      try:
        data = sock.recv(512)
      except socket.error as e:
        err = e.args[0]
        if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
          logging.critical("No data available")
          sock = None
          time.sleep(10)
        else:
          logging.critical("Error occured : %s" % (e))
          sock = None
          time.sleep(10)
      else:
        m = sbs1.SBS1Message(data)
        if m.isValid:
          if m.icao24 in observations:
            observations[m.icao24].update(m)
          else:
            observations[m.icao24] = Observation(m)
          if observations[m.icao24].isPresentable() and observations[m.icao24].updated:
            bridge.publish("/adsb/%s/json" % args.radar_name, json.dumps(observations[m.icao24].dict()))
            observations[m.icao24].updated = False
            observations[m.icao24].dump()


def main():
  global args
  parser = argparse.ArgumentParser(description='A Dump 1090 to MQTT bridge')

  parser.add_argument('-r', '--radar-name', help="name of radar, used as topic string /adsb/<radar>/json", default='radar')
  parser.add_argument('-m', '--mqtt-host', help="MQTT broker hostname", default='127.0.0.1')
  parser.add_argument('-p', '--mqtt-port', type=int, help="MQTT broker port number (default 1883)", default=1883)
  parser.add_argument('-u', '--mqtt-user', help="MQTT broker user")
  parser.add_argument('-a', '--mqtt-password', help="MQTT broker password")
  parser.add_argument('-H', '--dump1090-host', help="dump1090 hostname", default='127.0.0.1')
  parser.add_argument('-P', '--dump1090-port', type=int, help="dump1090 port number (default 30003)", default=30003)
  parser.add_argument('-pdb', '--planedb', dest='pdb_host', help="Plane database host")
  parser.add_argument('-v', '--verbose',  action="store_true", help="Verbose output")
  parser.add_argument('-l', '--logger', dest='log_host', help="Remote log host")
  parser.add_argument('-ca', '--ca-cert', help="path to CA Certificate roof file", default=None)
  parser.add_argument('-pe', '--pem-file', help="path to Pem file", default=None)
  parser.add_argument('-k', '--key', help="path to secret key file", default=None)
  parser.add_argument('-aws', '--aws-iot', help="path to secret key file", default=0)

  args = parser.parse_args()

  if args.verbose:
    loggingInit(logging.DEBUG, args.log_host)
  else:
    loggingInit(logging.INFO, args.log_host)

  if args.pdb_host:
    planedb.init(args.pdb_host)

  if args.ca_cert and args.pem_file and args.key:
    certificates = {
      'ca': args.ca_cert,
      'pem': args.pem_file,
      'key': args.key
    }
  bridge = mqtt_wrapper.bridge(host = args.mqtt_host, port = args.mqtt_port, client_id = "adsbclient-%d" % (random.randint(0, 65535)), user_id = args.mqtt_user, password = args.mqtt_password, certificates=certificates, AWS_IoT=args.aws_iot)
  thread = threading.Thread(target = mqttThread, args = (bridge,))
  thread.setDaemon(True)
  thread.start()
  thread = threading.Thread(target = adsbThread, args = (bridge,))
  thread.setDaemon(True)
  thread.start()

  numThreads = threading.active_count()
  while numThreads == threading.active_count():
    time.sleep(0.1)
  log.critical("Exiting")


# Ye ol main
main()
