#!/usr/bin/env python

import re
import sys
import json
import time

'''Read Swift log file and return a json file with its states and their
timings'''


# -----------------------------------------------------------------------------
def usage(msg=None, noexit=False):

    if msg:
        print "\nError: %s" % msg

    print """
    usage   : %s <swift_log_file>.json <swift_durations>.json

    """ % (sys.argv[0])

    if msg:
        sys.exit(1)

    if not noexit:
        sys.exit(0)


# -----------------------------------------------------------------------------
def get_timestamps(states, regs, flog):
    '''TBD
    '''

    for state in states:
        for line in flog:
            if not states[state]:
                states[state] = re.search(regs[state], line)

    for state in states:
        try:
            states[state] = states[state].group(1)
        except:
            states[state] = None

    return states


# -----------------------------------------------------------------------------
def to_epoch(tstamps):
    '''TBD
    '''

    ep_tstamps = {}
    pattern = '%Y-%m-%d %H:%M:%S'

    for state, dtime in tstamps.iteritems():
        if dtime:
            ep_tstamps[state] = int(time.mktime(time.strptime(dtime, pattern)))
        else:
            ep_tstamps[state] = None

    return ep_tstamps


# -----------------------------------------------------------------------------
def save_states(session, swift, tasks, fout):
    '''TDB.
    '''
    states = {'session': session, 'swift': swift, 'tasks': tasks}
    print states
    json.dump(states, fout, indent=4)


# ------------------------------------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) <= 2:
        usage("insufficient arguments -- need session ID")

    if len(sys.argv) > 3:
        usage("too many arguments -- no more than 2")

    # State model
    session = {}

    session['Started'] = None
    session['Finished'] = None
    session['n_tasks'] = None
    session['sid'] = None

    swift = {}

    swift['LoadWorkload'] = None
    swift['ExecuteWorkload'] = None
    swift['CleanupWorkload'] = None
    swift['Done'] = None

    tasks = {}

    tasks['New'] = None
    tasks['Executing'] = None
    tasks['Done'] = None

    # load log file and open output file
    flog = [line.strip() for line in open(sys.argv[1])]

    fout = open(sys.argv[2], 'w')

    # Regular expressions for each state change
    re_session = {}

    re_session['Started'] = re.compile("(\d+-\d+-\d+.*\d:\d+:\d+),\d+-\d+.*INFO.*Loader.*JAVA")
    re_session['Finished'] = re.compile("(\d+-\d+-\d+.*\d+:\d+),\d+-\d+.*INFO.*Loader.*Swift.*finished.*with.*no.*errors")
    re_session['n_tasks'] = re.compile("")
    re_session['sid'] = re.compile("")

    re_swift = {}

    re_swift['LoadWorkload'] = re.compile("")
    re_swift['ExecuteWorkload'] = re.compile("(\d+-\d+-\d+.*\d+:\d+),\d+-\d+.*DEBUG.*swift.*JOB_START")
    re_swift['CleanupWorkload'] = re.compile("(\d+-\d+-\d+.*\d+:\d+),\d+-\d+.*DEBUG.*swift.*Starting.*cleanups")
    re_swift['Done'] = re.compile("")

    re_tasks = {}

    re_tasks['New'] = re.compile("")
    re_tasks['Executing'] = re.compile("")
    re_tasks['Done'] = re.compile("")

    # Get states time stamps
    ts_session = get_timestamps(session, re_session, flog)
    ts_swift = get_timestamps(swift, re_swift, flog)
    ts_tasks = get_timestamps(tasks, re_tasks, flog)

    # Convert states timestamp to epoch
    ep_ts_session = to_epoch(ts_session)
    ep_ts_swift = to_epoch(ts_swift)
    ep_ts_tasks = to_epoch(ts_tasks)

    # Save states and times to a json file
    save_states(ep_ts_session, ep_ts_swift, ep_ts_tasks, fout)

    print ep_ts_session
    print ep_ts_swift
    print ep_ts_tasks
