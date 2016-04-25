import sys

from aimes.emgr.utils import *

__author__ = "Matteo Turilli"
__copyright__ = "Copyright 2015, The AIMES Project"
__license__ = "MIT"

# -----------------------------------------------------------------------------
def write_skeleton_conf(cfg, scale, cores, uniformity, fout):
    '''Write a skeleton configuration file with the set number/type/duration of
       tasks and stages.
    '''

    substitutes = dict()

    substitutes['SCALE'] = scale
    substitutes['CORES'] = cores[-1]

    if substitutes['CORES'] > 1:
        substitutes['TASK_TYPE'] = 'parallel'

    elif substitutes['CORES'] == 1:
        substitutes['TASK_TYPE'] = 'serial'

    else:
        print "ERROR: invalid number of cores per task: '%s'." % cores
        sys.exit(1)

    if uniformity == 'uniform':
        substitutes['UNIFORMITY_DURATION'] = "%s %s" % \
            (uniformity, cfg['skeleton_task_duration']['max'])

    # TODO: Calculate stdev and avg.
    elif uniformity == 'gauss':
        substitutes['UNIFORMITY_DURATION'] = "%s [%s, %s]" % \
            (uniformity, cfg['skeleton_task_duration']['avg'],
             cfg['skeleton_task_duration']['stdev'])

    else:
        print "ERROR: invalid task uniformity '%s' specified." % uniformity
        sys.exit(1)

    write_template(cfg['skeleton_template'], substitutes, fout)
