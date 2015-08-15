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

def list_workloads():
    r = requests.get("%s/swift/workloads/" % ep)
    print r.json()

def create_workload():
    r = requests.put("%s/swift/workloads/" % ep)
    print r.json()
    swid = r.json()['swid']
    print swid
    return swid

def add_cu(swid):
    global cnt
    cnt += 1
    data = {'cud': '{"executable":"/bin/exe_%03d"}' % cnt}
    r = requests.put("%s/swift/workloads/%s" % (ep, swid), data) 
    print r.json()

def dump_workload(swid):
    r = requests.get("%s/swift/workloads/%s" % (ep, swid)) 
    pprint.pprint(r.json())

def run_workload(swid):
    r = requests.put("%s/swift/workloads/%s/execute" % (ep, swid)) 
    print r.json()

def delete_workload(swid):
    r = requests.delete("%s/swift/workloads/%s" % (ep, swid)) 
    print r.json()


list_workloads()

# create a workload, and fill it with some CUs.  Then let some time expire so
# that the workload gets executed by the watcher 
swid = create_workload()
add_cu(swid)
add_cu(swid)
add_cu(swid)
time.sleep(6)

# Now we do the same again, on the same workload.  Only the second batch of
# units should get executed
add_cu(swid)
add_cu(swid)
add_cu(swid)
time.sleep(6)

# now we should have two batches of units executing.  We can submit the
# third one explicitly

add_cu(swid)
add_cu(swid)
add_cu(swid)
run_workload(swid)

list_workloads()
dump_workload(swid)

# delete_workload(swid)
list_workloads()

