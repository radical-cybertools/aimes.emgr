import os

from aimes.emgr.utils.misc import *

__author__ = "Matteo Turilli"
__copyright__ = "Copyright 2015, The AIMES Project"
__license__ = "MIT"


# -----------------------------------------------------------------------------
def initialize_log(run):
    '''Pass.
    '''

    f = open(run['files']['log'], "a", 1)

    # Title.
    separator = "=" * len("Run - "+run['tag'])

    print >> f, "%s\nRun - %s\n%s\n\n" % (separator, run['tag'], separator)

    f.flush()
    os.fsync(f)

    return f


# -----------------------------------------------------------------------------
def initialize_runtime(run):
    '''Pass.
    '''

    f = open(run['files']['runtime'], "a", 1)

    header    = '[%s] [%s] - Run %d/-%d' % (timestamp(), run['tag'],
                                            run['number'], run['left'])
    rdir      = '[%s] [%s] - Root: %s'   % (timestamp(), run['tag'],
                                            run['root'])
    separator = "-" * len(header)

    print >> f, "%s" % separator
    print >> f, "%s" % header
    print >> f, "%s" % rdir

    f.flush()
    os.fsync(f)

    return f


# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
def log_rp(run):
    '''Write a log file of the experiment run.
    '''

    f = run['log']

    title = 'Radical Pilot IDs'
    separator = "=" * len(title)
    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    # Session and managers ID.
    print >> f, "Session ID              : %s" % run['session_id']
    print >> f, "Pilot manager ID        : %s" % run['pilot_manager_id']
    print >> f, "Unit manager ID         : %s" % run['unit_manager_id']

    # Pilots.
    for pilot, resource in run['pilot_ids']:
        print >> f, "Pilot ID/resource : %s %s" % (pilot, resource)

    f.flush()
    os.fsync(f)


# -----------------------------------------------------------------------------
def log_skeleton(run, workflow):
    '''Pass.
    '''

    f = run['log']

    title = 'Skeleton'
    separator = "=" * len(title)
    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    print >> f, "Totals:"

    print >> f, "\tNumber of stages             : %d" % \
        len(workflow['skeleton'].stages)

    print >> f, "\tNumber of tasks              : %d" % \
        len(workflow['skeleton'].tasks)

    print >> f, "\tInput data                   : %.2f MB" % \
        float((workflow['skeleton_input_data']/1024)/1024)

    print >> f, "\tOutput data                  : %.2f MB" % \
        float((workflow['skeleton_output_data']/1024)/1024)

    print >> f, "\tLongest task execution time  : %s seconds" % \
        workflow['task_time']['max']

    print >> f, "\tShortest task execution time : %s seconds" % \
        workflow['task_time']['min']

    print >> f, "\tLargest compute task         : %s core(s)" % \
        workflow['task_compute']['max']

    print >> f, "\tSmallest compute task        : %s core(s)" % \
        workflow['task_compute']['min']

    print >> f, ''

    for stage in workflow['skeleton'].stages:

        print >> f, "%s:" % stage.name

        print >> f, "\tNumber of tasks   : %d" % len(stage.tasks)
        print >> f, "\tTime distribution : %s" % run['uniformity']

        print >> f, "\tInput files       : %d for a total of %.2f MB" % \
            (workflow[stage.name]['input'][0],
             float((workflow[stage.name]['input'][1]/1024.0)/1024.0))

        print >> f, "\tOutput files      : %d for a total of %.2f MB" % \
            (workflow[stage.name]['output'][0],
             float((workflow[stage.name]['output'][1]/1024.0)/1024.0))

    print >> f, ''

    print >> f, "Execution boundaries:"

    print >> f, "\tLowest number of cores  : %s" % \
        workflow['task_compute']['min']

    print >> f, "\tlongest execution time  : %s seconds" % \
        workflow['stages_time']['max']

    print >> f, "\tHighest number of cores : %s" % \
        workflow['stages_compute']['max']

    print >> f, "\tshortest execution time : %s seconds" % \
        workflow['task_time']['max']

    print >> f, "\n"

    f.flush()
    os.fsync(f)


# -----------------------------------------------------------------------------
def log_bundle(run, resources):
    '''Pass.
    '''

    f = run['log']

    # report.header("Skeleton Workflow S01")
    title = 'Bundle'
    separator = "=" * len(title)
    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    # Report back to the demo about the available resource bundle.
    print >> f, "Target resources IDs : %s" % ', '.join(
        map(str, resources['resource_ids']))

    print >> f, "Total core capacity  : %i" % resources['core_capacity']

    print >> f, "\n"

    f.flush()
    os.fsync(f)


# -----------------------------------------------------------------------------
def log_execution_stategy(cfg, run, strategy):
    '''Pass.
    '''

    f = run['log']

    title = 'Execution Strategy'
    separator = "=" * len(title)
    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    print >> f, "Configurations:"

    if 'supported' in cfg['bundle']['resources']:
        print "I am here: %s" % cfg['bundle']['resources']['supported']
        print >> f, "\tTarget resource for early binding : %s" %\
            cfg['bundle']['resources']['supported']

        print >> f, "\tTarget resources for late binding : %s" %\
            ', '.join(map(str, cfg['bundle']['resources']['supported'].keys()))

    if 'supported' in cfg['bundle']['resources']:
        print >> f, "\tTarget resource for early binding : %s" %\
            cfg['bundle']['resources']['unsupported']

        print >> f, "\tTarget resources for late binding : %s" %\
            ', '.join(map(str, cfg['bundle']['resources']['unsupported'].keys()))

    print >> f, "\tType of task-to-resource binding  : %s" %\
        run['binding']

    print >> f, ''

    print >> f, "Heuristics:"

    print >> f, "\tDegree of concurrency for task execution : %s%%" %\
        strategy['heuristic']['percentage_concurrency']

    print >> f, "\tPercentage of bundle resources targeted  : %s%%" %\
        strategy['heuristic']['percentage_resources']

    print >> f, ''

    print >> f, "Inferences:"

    print >> f, "\tNumber of target resources      : %d" % \
        len(strategy['inference']['target_resources'])

    print >> f, "\tTarget resource(s) for pilot(s) : %s" % \
        ', '.join(map(str, strategy['inference']['target_resources']))

    print >> f, "\tNumber of pilots                : %d" % \
        strategy['inference']['number_pilots']

    print >> f, "\tTotal workflow number of cores  : %s" % \
        strategy['inference']['cores_workload']

    print >> f, "\tType of scheduler for RP        : %s" % \
        strategy['inference']['rp_scheduler']

    print >> f, "\tTotal workflow compute time     : %s seconds" % \
        strategy['inference']['compute_time_workload']

    print >> f, "\tTotal workflow staging time     : %s seconds" % \
        strategy['inference']['staging_time_workload']

    print >> f, "\tTotal RP overhead time          : %s seconds" % \
        strategy['inference']['rp_overhead_time_workload']

    print >> f, "\n"

    f.flush()
    os.fsync(f)


# -----------------------------------------------------------------------------
def log_pilot_descriptions(run):
    '''Pass.
    '''

    f = run['log']

    title = 'Pilot Descriptions'
    separator = "=" * len(title)
    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    for pdesc in run['pdescs']:

        print >> f, "%s:" % pdesc.resource
        print >> f, "\tAllocation; None -> RP default : %s" % pdesc.project
        print >> f, "\tQueue; None -> RP default      : %s" % pdesc.queue
        print >> f, "\tNumber of cores                : %s" % pdesc.cores
        print >> f, "\tWalltime in minutes            : %s" % pdesc.runtime
        print >> f, "\tStop once the workflow is done : %s" % pdesc.cleanup

        print >> f, ''

    print >> f, "\n"

    f.flush()
    os.fsync(f)


# -----------------------------------------------------------------------------
def log_cu_descriptions(cfg, run, workflow, cuds):
    '''Pass.
    '''

    f = run['log']

    title     = 'Compute Unit Descriptions'
    separator = "=" * len(title)
    cuds      = [j for i in cuds.values() for j in i]

    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    if cfg['workload_type'] == 'skeleton':
        print >> f, "Total tasks submitted  : %d" % workflow['skeleton_tasks']

    print >> f, "Total CU translated    : %d" % len(cuds)

    for core in cfg['cores']:
        print >> f, "Total CU with %s cores : %s" % (core, cuds.count(core))

    print >> f, ''

    print >> f, "Print the first units for reference:"

    # for cud in cuds[0:4]:
    if cfg['workload_type'] == 'skeleton':

        for cud in cuds:

            print >> f, "%s:" % cud.name
            print >> f, "\tExecutable           : %s" % cud.executable
            print >> f, "\tArguments executable : %s" % cud.arguments
            print >> f, "\tNumber of cores      : %s" % cud.cores
            print >> f, "\tPre-execution        : %s" % cud.pre_exec
            print >> f, "\tInput staging        : %s" % cud.input_staging
            print >> f, "\tOutput staging       : %s" % cud.output_staging
            print >> f, "\tCleanup              : %s" % cud.cleanup

            print >> f, ''

        print >> f, "\n"

    f.flush()
    os.fsync(f)

# -----------------------------------------------------------------------------
def record_run_state(run):
    '''Pass.
    '''

    f = run['runtime']

    print >> f, "[%s] [%s] - State: %s" % (timestamp(), run['tag'],
                                           run['state'])

    f.flush()
    os.fsync(f)


# -----------------------------------------------------------------------------
def record_run_session(run):
    '''Pass.
    '''

    f = run['runtime']

    print >> f, "[%s] [%s] - Session: %s" % (timestamp(), run['tag'],
                                             run['session_id'])

    f.flush()
    os.fsync(f)
