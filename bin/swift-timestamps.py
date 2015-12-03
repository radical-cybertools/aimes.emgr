#!/usr/bin/env python

import re
import sys
import json
import time

'''Read Swift log file and return a json file with its states and their
timings'''


class Run(object):
    def __init__(self, conf):
        self.conf = conf
        self.logs = [line.strip() for line in open(conf['file_logs'])]
        self.reid = conf['re']['runid']
        self.dtpattern = conf['date_time_pattern']
        self.id = self._get_id()
        self.tasks = self._get_tasks()
        self.states = []

    def _get_id(self):
        runid = None
        regex = re.compile(self.reid)
        for line in self.logs:
            if runid:
                break
            m = re.match(regex, line)
            if m and not runid:
                runid = m.group(1)
        return runid

    def _get_tasks(self):
        ids = []
        tasks = []
        taskid = re.compile(self.conf['re']['taskid'])
        for line in self.logs:
            m = re.match(taskid, line)
            if m and m.group(1) not in ids:
                ids.append(m.group(1))
                tasks.append(Task(m.group(1), self))
        return tasks

    def _make_re(self, res):
        re = ''
        for tag in res:
            re += run.conf['re'][tag]+'.*'
        return re

    def add_state(self, name, res):
        re = self._make_re(res)
        self.states.append(State(name, re, self))

    def save_to_json(self):
        d = {}
        d["Run"] = {"ID": self.id}
        d["Tasks"] = {}
        for state in self.states:
            d["Run"][state.name] = state.tstamp.epoch
        for task in self.tasks:
            d["Tasks"][task.id] = {}
            for state in task.states:
                d["Tasks"][task.id][state.name] = state.tstamp.epoch
        fout = open(conf['file_json'], 'w')
        json.dump(d, fout, indent=4)


class Task(object):
    def __init__(self, tid, run):
        self.run = run
        self.id = tid
        self.states = []

    def _make_re(self, res):
        re = ''
        for tag in res:
            re += run.conf['re'][tag]+'.*'
        return re

    def add_state(self, name, res, taskid=None, code=None):
        re = self._make_re(res)
        if taskid:
            re += '.*'+taskid
        if code:
            re += '.*status=%s' % code
        self.states.append(State(name, re, self.run))


class State(object):
    def __init__(self, name, re_state, run):
        self.name = name
        self.tstamp = TimeStamp(re_state, self, run)


class TimeStamp(object):
    def __init__(self, re_state, state, run):
        self.regex = re.compile(re_state)
        self.state = state
        self.run = run
        self.stamp = self._get_stamp()
        self.epoch = int(time.mktime(time.strptime(self.stamp, 
                         self.run.dtpattern)))

    def _get_stamp(self):
        stamp = None
        for line in self.run.logs:
            if stamp:
                break
            m = re.match(self.regex, line)
            if m:
                stamp = "%s %s" % (m.group(1), m.group(2))
        return stamp


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


# ------------------------------------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) <= 2:
        usage("insufficient arguments -- need session ID")

    if len(sys.argv) > 3:
        usage("too many arguments -- no more than 2")

    conf = {}
    conf['re'] = {}
    conf['file_logs'] = sys.argv[1]
    conf['file_json'] = sys.argv[2]
    conf['tcodes'] = {'Submitting': 8, 'Submitted': 1, 'Active': 2,
                      'Completed': 7}
    conf['date_time_pattern'] = "%Y-%m-%d %H:%M:%S"
    conf['re']['date'] = "(\d+-\d+-\d+)"
    conf['re']['time'] = "(\d:\d+:\d+),\d+[-,+]\d+"
    conf['re']['start'] = "INFO.*Loader.*JAVA"
    conf['re']['finish'] = "INFO.*Loader.*Swift.*finished.*with.*no.*errors"
    conf['re']['runid'] = ".*INFO.*Loader.*RUN_ID.*(run\d{3}).*"
    conf['re']['taskid'] = ".*taskid=urn:(R-\d+[-,x]\d+[-,x]\d+).*"
    conf['re']['tasknew'] = "INFO.*Execute.*JOB_TASK.*jobid.*taskid=urn:"
    conf['re']['tasksts'] = "TASK_STATUS_CHANGE.*taskid=urn:"

    run = Run(conf)
    run.add_state('Start', ['date', 'time', 'start'])
    run.add_state('Finish', ['date', 'time', 'finish'])

    for task in run.tasks:
        task.add_state('New', ['date', 'time', 'tasknew'], task.id)
        for name, code in conf['tcodes'].iteritems():
            task.add_state(name, ['date', 'time', 'tasksts'], task.id, code)

    run.save_to_json()
