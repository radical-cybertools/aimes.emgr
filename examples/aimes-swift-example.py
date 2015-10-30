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

if sys.argv[1][-1:] == '/':
    print "\n\n\trest endpoint %s must not end by '/'\n\n" % sys.argv[1]
    sys.exit(-1)

ep = sys.argv[1]


def list_sessions():
    r = requests.get("%s/swift/sessions/" % ep)
    print r
    print r.json()


def create_session():
    r = requests.put("%s/swift/sessions/" % ep)
    print r.json()
    emgr_sid = r.json()['emgr_sid']
    print emgr_sid
    return emgr_sid


def add_task(emgr_sid):
    global cnt
    cnt += 1
    cud = {"executable" : "/bin/sleep",
           "arguments"  : ["%d" % cnt],
           "cores"      : 1}
    data = {'td': json.dumps(cud)}
    r = requests.put("%s/swift/sessions/%s" % (ep, emgr_sid), data)
    print r.json()
    return r.json()['emgr_tid']


def dump_session(emgr_sid):
    r = requests.get("%s/swift/sessions/%s" % (ep, emgr_sid))
    pprint.pprint(r.json())


def check_task(emgr_sid, emgr_tid):
    r = requests.get("%s/swift/sessions/%s/%s" % (ep, emgr_sid, emgr_tid))
  # pprint.pprint(r.json())
    print 'task %s: %s' % (emgr_tid, r.json()['result']['state'])

    if r.json()['result']['state'].lower() in ['done', 'canceled', 'failed']:
        return True
    else:
        return False


def run_session(emgr_sid):
    r = requests.put("%s/swift/sessions/%s/execute" % (ep, emgr_sid))
    print r.json()


def delete_session(emgr_sid):
    r = requests.delete("%s/swift/sessions/%s" % (ep, emgr_sid))
    print r.json()


# create a session, and begin submitting tasks.  Then let some time expire so
# that the tasks get executed by the watcher
print ' ---------- create session'
emgr_sid = create_session()

print ' ---------- list sessions'
list_sessions()
tids = list()

print ' ---------- submit tasks'
tids.append(add_task(emgr_sid))
tids.append(add_task(emgr_sid))
tids.append(add_task(emgr_sid))

print ' ---------- sleep'
time.sleep(6)

# Now we do the same again, and this batch should get executed in some seconds,
# too.
print ' ---------- submit tasks'
tids.append(add_task(emgr_sid))
tids.append(add_task(emgr_sid))
tids.append(add_task(emgr_sid))

while True:

    # we wait for all tasks to finish
    all_finished = True
    for tid in tids:
        if not check_task(emgr_sid, tid):
            all_finished = False
    if all_finished:
        break
    else:
        print ' ---------- sleep 10'
        time.sleep (10)

print ' ---------- all tasks are final'
print ' ---------- list sessions, dump this session'
list_sessions()
dump_session(emgr_sid)

# print ' ---------- delete this session, list sessions'
# delete_session(emgr_sid)
# list_sessions()

