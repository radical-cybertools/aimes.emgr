#!/usr/bin/env python

# this code talks to a swift-aimes rest service and runs a simple workload.  The
# first argument is the service endpoint.

import sys
import json
import time
import pprint
import requests

cnt = 0

if len(sys.argv) < 2:
    print "\n\n\tusage: %s <rest-endpoint>\n\n" % sys.argv[0]
    sys.exit(-1)

ep = sys.argv[1]

def list_sessions():
    r = requests.get("%s/swift/sessions/" % ep)
    print r.json()

def create_session():
    r = requests.put("%s/swift/sessions/" % ep)
    print r.json()
    ssid = r.json()['ssid']
    print ssid
    return ssid

def add_task(ssid):
    global cnt
    cnt += 1
    data = {'td': '{"executable":"/bin/exe_%03d"}' % cnt}
    r = requests.put("%s/swift/sessions/%s" % (ep, ssid), data) 
    print r.json()
    return r.json()['stid']

def dump_session(ssid):
    r = requests.get("%s/swift/sessions/%s" % (ep, ssid)) 
    pprint.pprint(r.json())

def check_task(ssid, stid):
    r = requests.get("%s/swift/sessions/%s/%s" % (ep, ssid, stid)) 
    pprint.pprint(r.json())

def run_session(ssid):
    r = requests.put("%s/swift/sessions/%s/execute" % (ep, ssid)) 
    print r.json()

def delete_session(ssid):
    r = requests.delete("%s/swift/sessions/%s" % (ep, ssid)) 
    print r.json()


print ' ---------- list sessions'
list_sessions()
tids = list()

# create a session, and begin submitting tasks.  Then let some time expire so
# that the tasks get executed by the watcher 
print ' ---------- create session'
ssid = create_session()

print ' ---------- submit tasks'
tids.append(add_task(ssid))
tids.append(add_task(ssid))
tids.append(add_task(ssid))

print ' ---------- sleep'
time.sleep(6)

# Now we do the same again, and this batch should get executed in some seconds,
# too.
print ' ---------- submit tasks'
tids.append(add_task(ssid))
tids.append(add_task(ssid))
tids.append(add_task(ssid))

for i in range(3):

    print ' ---------- sleep 10'
    time.sleep (10)

    print ' ---------- check tasks'
    check_task(ssid, tids[ 0])
    check_task(ssid, tids[-1])

print ' ---------- list sessions, dump this session'
list_sessions()
dump_session(ssid)

# print ' ---------- delete this session, list sessions'
# delete_session(ssid)
# list_sessions()

