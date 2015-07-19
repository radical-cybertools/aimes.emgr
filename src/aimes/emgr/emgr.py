
"""An experiment is coded as a sequence of runs. Each run is repeated for a
    fixed number of times so to derive appropriate relative errors.

    State model for run:

    States: [DESCRIBED, READY, ACTIVE, FAILED, DONE]

    * A run is DESCRIBED when its properties are defined.
    * A run becomes READY when its description is queued into the READY queue.
    * A run becomes ACTIVE when it starts to be processed for execution.
    * A run has two final states:
        - A run becomes 'FAILED' when an exception is catch during its
          execution.
        - A run becomes 'DONE' when its execution  successfully terminated.
"""

import os
import sys
import math
import time
import Queue
import random
import smtplib
import datetime
import traceback

import saga as rs
import radical.utils as ru
import radical.pilot as rp

import aimes.bundle
import aimes.skeleton

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from string import Template

__author__ = "Matteo Turilli, Andre Merzky"
__copyright__ = "Copyright 2015, The AIMES Project"
__license__ = "MIT"
__credits__ = ["Andre Merzky"]


# -----------------------------------------------------------------------------
# SETTING UP
# -----------------------------------------------------------------------------
def create_run_environment(cfg, run_cfg, tracker, q_qsize):
    '''Pass
    '''

    run = {'scale'     : run_cfg[0],
           'binding'   : run_cfg[1],
           'uniformity': run_cfg[2],
           'iteration' : run_cfg[3],
           'rerun'     : run_cfg[4],
           'cores'     : run_cfg[5],
           'number'    : tracker,
           'left'      : q_qsize}

    tag = "scale %s; binding %s; distribution %s; iteration %s; rerun %s"

    run['tag'] = tag % (run['scale'], run['binding'], run['uniformity'],
                        run['iteration'], run['rerun'])

    root = "run-%s_%s_%s_%s/"

    run['root'] = root % (run['scale'], run['binding'], run['uniformity'],
                          run['iteration'])

    if not os.path.exists(run['root']):
        os.makedirs(run['root'])

    # Define the constants needed by executing the experimet run.
    run['files'] = {'log'     : run['root'] + 'log.txt',
                    'runtime' : cfg['experiment_log'],
                    'skeleton': run['root'] + 'skeleton.conf',
                    'bundle'  : run['root'] + 'bundle.conf',
                    'email'   : run['root'] + 'email.body',
                    'dbkp'    : run['root'] + 'dbkp.tar.bz2',
                    'stats'   : run['root'] + 'stats.txt',
                    'diagram' : run['root']}

    # Initialize log files for the application.
    run['log'] = initialize_log(run)

    # FIXME. This need to be initialized for the experiment.
    run['runtime'] = initialize_runtime(run)

    # Write the configuration file of the skeleton for this run.
    write_skeleton_conf(cfg, run['scale'], run['cores'], run['uniformity'],
                        run['files']['skeleton'])

    # Write the configuration file of the skeleton for this run.
    write_bundle_conf(cfg, run['binding'], run['files']['bundle'])

    return run


# -----------------------------------------------------------------------------
def finalize_run_environment(cfg, run):
    '''Pass
    '''

    run['runtime'].close()
    run['log'].close()


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
            (uniformity, cfg['skeleton_task_duration']['avg'])

    elif uniformity == 'gauss':
        substitutes['UNIFORMITY_DURATION'] = "%s [%s, %s]" % \
            (uniformity, cfg['skeleton_task_duration']['avg'],
             cfg['skeleton_task_duration']['stdev'])

    else:
        print "ERROR: invalid task uniformity '%s' specified." % uniformity
        sys.exit(1)

    write_template(cfg['skeleton_template'], substitutes, fout)


# -----------------------------------------------------------------------------
def write_bundle_conf(cfg, binding, fout):
    '''Write a bundle configuration file with the set number/type/duration of
       tasks and stages.
    '''

    substitutes = {'RESOURCE_LIST': ''}

    entry_template = "cluster_type=%s hostname=%s username=%s\n"

    # cluster_type=config
    # hostname=cameo.merzky.net
    # config=etc/resource_config.json
    entry_template_unsupported = "cluster_type=%s hostname=%s config=%s\n"

    if cfg['bundle_resources']:
        for resource, scheduler in cfg['bundle_resources'].iteritems():

            if binding == 'early' and cfg['bundle_resource'] in resource:
                substitutes['RESOURCE_LIST'] = entry_template % \
                    (scheduler, resource, cfg['bundle_username'])
                break

            substitutes['RESOURCE_LIST'] += entry_template % \
                (scheduler, resource, cfg['bundle_username'])

    if cfg['bundle_unsupported']:
        for resource, properties in cfg['bundle_unsupported'].iteritems():

            substitutes['RESOURCE_LIST'] += entry_template_unsupported % \
                (properties['sched'], resource, properties['fconf'])

    write_template(cfg['bundle_template'], substitutes, fout)


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
# DERIVING
# -----------------------------------------------------------------------------
def derive_workflow(cfg, skeleton, run):
    '''Pass
    '''

    workflow = {'skeleton_input_data': 0.0,
                'skeleton_output_data': 0.0}

    # Calculate total data size of the given workflow.
    for task in skeleton.tasks:
        for i in task.inputs:
            workflow['skeleton_input_data'] += float(i['size'])

    for task in skeleton.tasks:
        for o in task.outputs:
            workflow['skeleton_output_data'] += float(o['size'])

    for stage in skeleton.stages:

        # Find out how many input/output files and how much space they require
        # for this stage.
        stage_input_data = 0
        stage_input_files = 0

        for task in skeleton.tasks:
            if task.stage().name == stage.name:
                for i in task.inputs:
                    stage_input_data += float(i['size'])
                    stage_input_files += 1

        stage_output_data = 0
        stage_output_files = 0

        for task in skeleton.tasks:
            if task.stage().name == stage.name:
                for o in task.outputs:
                    stage_output_data += float(o['size'])
                    stage_output_files += 1

        workflow[stage.name] = {'input' : [stage_input_files,
                                           stage_input_data],
                                'output': [stage_output_files,
                                           stage_output_data]}

        # FIXME. This is redundant, it will not be necessary once all the above
        # will be available directly via the skeleton API. The dictionary
        # workflow will then be eliminated.
        workflow['skeleton_tasks'] = len(skeleton.tasks)

        # Execution Boundaries: calculate the min/max time taken by each stage
        # to execute and the mix/max amount of cores needed. Factor data
        # transfer time into min/max time. Note: Max(compute) <=> Min(time) &
        # Min(compute) <=> Max(time)
        workflow['stages_compute'] = {}
        workflow['stages_time'] = {}
        workflow['task_compute'] = {}
        workflow['task_time'] = {}
        # workflow['task_compute_sorted'] = []
        workflow['task_time_sorted'] = []

        for task in skeleton.tasks:

            # TASK DURATION
            #
            # Note: Gaussian distributions will produce negative lengths. This
            # makes no sense when applied to task duration even if it is
            # formally correct. The following produces a pseudo-gaussian
            # distribution eliminating negative durations and setting a minimal
            # duration for each task.
            if task.length < cfg['skeleton_task_duration']['min']:

                workflow['task_time_sorted'].append(
                    cfg['skeleton_task_duration']['min'])

            elif task.length > cfg['skeleton_task_duration']['max']:

                workflow['task_time_sorted'].append(
                    cfg['skeleton_task_duration']['max'])

            else:
                workflow['task_time_sorted'].append(task.length)

            # TASK SIZE (#CORES)
            #
            # The number of cores per task is set in the skeleton conf file as
            # 'Num_Processes'.
            # workflow['task_compute_sorted'].append(task.cores)

        # workflow['task_compute_sorted'].sort()
        workflow['task_time_sorted'].sort()

        # Skeletons do not allow to partition the set of tasks of each stage in
        # set of tasks with different number of cores. The following is a local
        # implementation of this functionality.
        #
        # NOTE: the following is not stage safe!
        total_cores = 0
        task_partition = math.ceil(run['scale']/float(len(cfg['cores'])))

        for core in cfg['cores']:
            total_cores += task_partition * core

        workflow['stages_compute']['max'] = total_cores
        workflow['task_compute']['max'] = cfg['cores'][-1]
        workflow['task_compute']['min'] = cfg['cores'][0]

        workflow['stages_time']['max'] = sum(workflow['task_time_sorted'])
        workflow['task_time']['max'] = workflow['task_time_sorted'][-1]
        workflow['task_time']['min'] = workflow['task_time_sorted'][0]

        workflow['skeleton'] = skeleton

        return workflow


# -----------------------------------------------------------------------------
def derive_resources(cfg, bundle):
    '''Collect information about the resources to plan the execution strategy.
    '''

    resources = {}

    resources['bandwidth_in'] = dict()
    resources['bandwidth_out'] = dict()

    # Get network bandwidth for each resource.
    for resource_name in bundle.resources:
        resource = bundle.resources[resource_name]

        resources['bandwidth_in'][resource.name] = resource.get_bandwidth(
            cfg['bundle_origin'], 'in')

        resources['bandwidth_out'][resource.name] = resource.get_bandwidth(
            cfg['bundle_origin'], 'out')

    # Get the total core capacity offered by the default queues of the target
    # resources.
    resources['core_capacity'] = 0

    for r_name in bundle.resources:
            resource = bundle.resources[r_name]

            for q_name in resource.queues:
                queue = resource.queues[q_name]

                if (q_name == 'normal' or
                    q_name == 'batch' or
                    q_name == 'default' or
                    q_name == 'regular'):
                        resources['core_capacity'] += queue.num_procs_limit

    # Resource IDs.
    resources['resource_ids'] = [
        str(bundle.resources[resource].name) for resource in bundle.resources]

    resources['bundle'] = bundle

    return resources


# -----------------------------------------------------------------------------
def derive_execution_stategy(cfg, workflow, resources, run):
    '''Pass.
    '''

    strategy = {}
    strategy['heuristic'] = {}
    strategy['inference'] = {}

    # Degree of concurrency. Question: what amount of concurrent execution
    # minimizes TTC?
    strategy['heuristic']['percentage_concurrency'] = 100

    # Number of resources. Question: what is the number of resources that when
    # used to execute the tasks of the workflow minimize the TTC?
    strategy['heuristic']['percentage_resources'] = 100

    # CHOOSE RESOURCES: Get the resources from the bundle.
    strategy['inference']['target_resources'] = list()

    if cfg['bundle_resources']:
        for resource in cfg['bundle_resources'].keys():

            if run['binding'] == 'early' and cfg['bundle_resource'] in resource:
                strategy['inference']['target_resources'].append(
                    uri_to_tag(resource))
                break

            strategy['inference']['target_resources'].append(uri_to_tag(resource))

    if cfg['bundle_unsupported']:
        for resource in cfg['bundle_unsupported'].keys():

            if run['binding'] == 'early' and cfg['bundle_unsupported'] in resource:
                strategy['inference']['target_resources'].append(
                    uri_to_tag(resource))
                break

            strategy['inference']['target_resources'].append(uri_to_tag(resource))

    # CHOOSE NUMBER OF PILOTS: Adopt an heuristics that tells us how many
    # concurrent resources we should choose given the execution time
    # boundaries. We assume that task concurrency should always be maximized we
    # may decide that we want to start with #pilots = #resources to which we
    # have access.
    if strategy['heuristic']['percentage_resources'] == 100:
        strategy['inference']['number_pilots'] = len(
            strategy['inference']['target_resources'])

    # CHOOSE THE SCHEDULER FOR THE CUs: Depending on whether we have multiple
    # pilot and on what metric needs to bo min/maximized. In this demo we
    # minimize TTC so we choose backfilling. Do we have a default scheduler? If
    # so, an else is superfluous.
    if len(strategy['inference']['target_resources']) > 1:
        strategy['inference']['rp_scheduler'] = 'SCHED_BACKFILLING'
    else:
        strategy['inference']['rp_scheduler'] = 'SCHED_DIRECT_SUBMISSION'

    # TIME COMPONENTS OF EACH PILOT WALLTIME:
    #
    # - COMPUTE TIME: the time taken by the tasks of each single stage of the
    #   workflow to execute on a pilot of the resource overlay, given the
    #   decided degree of concurrency.
    # . Requirements: we need to be able to run all the tasks on a single
    #   pilot; i.e. the worse case scenario in which a single pilot is
    #   available for enough time that the whole workflow can be run at 1/n of
    #   the optimal concurrency that would be achieved by having all the n
    #   pilots available.
    # . Implicit assumption: pilots are heterogeneous - all have the same
    #   walltime and number of cores.
    # . Formula: after sorting the length of all the tasks, the walltime
    #   accounting for the described worse case scenario is the sum of the n
    #   longest tasks with n = the number of pilots instantiated.
    strategy['inference']['compute_time_workflow'] = (
        sum(workflow['task_time_sorted'][
            -len(strategy['inference']['target_resources']):]))

    # - STAGING TIME: the time needed to move the I/O files of each task (that
    #   will be) bound to each pilot. We assume a conservative 5 seconds to
    #   transfer 1 MB but this value will have to be taken dynamically from a
    #   monitoring system testing the transfer speed between two given points -
    #   origin and destination.
    strategy['inference']['staging_time_workflow'] = (
        (((workflow['skeleton_input_data'] +
           workflow['skeleton_output_data']) / 1024) / 1024) * 5)

    # - RP OVERHEAD TIME: the time taken by RP to bootstrap and manage each CU
    #   for each pilot. This value needs to be assessed inferred by a
    #   performance model of RP.
    strategy['inference']['rp_overhead_time_workflow'] = (
        600 + workflow['skeleton_tasks'] * 4)

    # PILOT DESCRIPTIONS: Maximal concurrency is achieved by having 1 core for
    # each core needed by each task of the given workflow. A minimal
    # concurrency will need to be calculated so to guarantee the availability
    # of the minimal amount of cores needed by the largest task (i.e. the tasks
    # that need the largest number of cores in order to be executed).
    strategy['inference']['cores_workflow'] = math.ceil(
        (workflow['stages_compute']['max'] *
         strategy['heuristic']['percentage_concurrency']) / 100.0)

    return strategy


# -----------------------------------------------------------------------------
def derive_pilot_descriptions(cfg, strategy):
    '''Pass.
    '''

    pdescs = []

    # FIXME. Create a pilot description for each resource. Assumption: there is
    # a single pilot for each resource and the number of pilots is the same of
    # the number of resources.
    for resource in strategy['inference']['target_resources']:

        if not resource in cfg['resources']:
            print "ERROR: No configuration for resource %s." % resource
            sys.exit(1)

        pdesc = rp.ComputePilotDescription()

        pdesc.project  = cfg['resources'][resource]['project']
        pdesc.resource = resource  # label

        # Select a specific queue for hopper. This will become another
        # decision point inferred from queue information and inferred
        # duration of the workflow.
        if 'hopper' in pdesc.resource:
            # pdesc.queue = 'regular'
            # Use queue dedicated to CCM scheduler
            pdesc.queue = 'ccm_queue'

        # We assume a uniform distribution of the total amount of cores
        # across all the available pilots. Future optimizations may
        # take into consideration properties of the resources that
        # would justify a biased/proportional distribution of the
        # cores.
        #
        # NOTE: Evenly distribution of tasks with a heterogeneous number of
        # cores is not guaranteed by the backfilling scheduler. TTC will be
        # affected at relevant scales as the core utilization per pilot will be
        # different and indeterministic with the current scheduling
        # implementation.
        pdesc.cores = math.ceil(
            float(strategy['inference']['cores_workflow'] /
                  strategy['inference']['number_pilots']))

        # Aggregate time components for the pilot walltime.
        pdesc.runtime = math.ceil(
            (strategy['inference']['compute_time_workflow'] +
             strategy['inference']['staging_time_workflow'] +
             strategy['inference']['rp_overhead_time_workflow'])) / 60.0

        # We clean the pilot files once execution is done.
        pdesc.cleanup = True

        pdescs.append(pdesc)

    return pdescs


# -----------------------------------------------------------------------------
def derive_cu_descriptions(run, workflow):
    '''Derives CU from the tasks on n stages of the given workflow.
    '''

    cuds = {}
    cucounters = {}

    # Initialize CU counters.
    for core in cfg['cores']:
        cucounters[core] = 1

    # Translate skeleton tasks into CUs.
    for stage in workflow['skeleton'].stages:

        cuds[stage.name] = list()

        for task in stage.tasks:

            cud = rp.ComputeUnitDescription()

            # Introduce heterogeneity in #cores of the workload. It assumes
            # equal number of tasks for each core size. The number of tasks is
            # derived from the scale of the workload.
            for counter, value in cucounters.iteritems():
                if value <= math.ceil(run['scale']/float(len(cfg['cores']))):
                    cud.cores = counter
                    cucounters[counter] += 1
                    break

            cud.name = stage.name+'_'+task.name
            cud.executable = task.command.split()[0]
            cud.arguments = task.command.split()[1:]

            # Introduce heterogeneity in the skeleton command line.
            cud.arguments[1] = str(cud.cores)

            cud.pre_exec = list()
            cud.input_staging = list()
            cud.output_staging = list()

            # make sure the task is compiled on the fly
            # FIXME: it does not work with trestles as it assumes only a
            # working cc compiler.
            # cud.input_staging.append (aimes.skeleton.TASK_LOCATION)
            # cud.pre_exec.append      (aimes.skeleton.TASK_COMPILE)

            iodirs = task.command.split()[9:-1]
            odir = iodirs[-1].split('/')[0]

            for i in range(0, len(iodirs)):

                if iodirs[i].split('/')[0] != odir:
                    idir = iodirs[i].split('/')[0]
                    break

            for i in task.inputs:
                cud.input_staging.append({
                    'source': idir + '/' + i['name'],
                    'target': idir + '/' + i['name'],
                    'flags': rp.CREATE_PARENTS
                    })

            for o in task.outputs:
                cud.output_staging.append({
                    'source': odir + '/' + o['name'],
                    'target': odir + '/' + o['name'],
                    'flags': rp.CREATE_PARENTS
                    })

            # FIXME: restartable CUs still do not work.
            #cud.restartable = True
            cud.cleanup = True

            cuds[stage.name].append(cud)

    # Shuffle the list of CU descriptions so to minimize the impact of the
    # list ordering on the ordering of the scheduling on one or more pilots.
    random.shuffle(cuds)

    return cuds


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

    if cfg['bundle_resource']:
        print "I am here: %s" % cfg['bundle_resource']
        print >> f, "\tTarget resource for early binding : %s" %\
            cfg['bundle_resource']

        print >> f, "\tTarget resources for late binding : %s" %\
            ', '.join(map(str, cfg['bundle_resources'].keys()))

    if cfg['bundle_unsupported']:
        print >> f, "\tTarget resource for early binding : %s" %\
            cfg['bundle_unsupported']

        print >> f, "\tTarget resources for late binding : %s" %\
            ', '.join(map(str, cfg['bundle_unsupported'].keys()))

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
        strategy['inference']['cores_workflow']

    print >> f, "\tType of scheduler for RP        : %s" % \
        strategy['inference']['rp_scheduler']

    print >> f, "\tTotal workflow compute time     : %s seconds" % \
        strategy['inference']['compute_time_workflow']

    print >> f, "\tTotal workflow staging time     : %s seconds" % \
        strategy['inference']['staging_time_workflow']

    print >> f, "\tTotal RP overhead time          : %s seconds" % \
        strategy['inference']['rp_overhead_time_workflow']

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
def log_cu_descriptions(run, workflow):
    '''Pass.
    '''

    f = run['log']

    title     = 'Compute Unit Descriptions'
    separator = "=" * len(title)
    cuds      = [j for i in run['cuds'].values() for j in i]

    print >> f, "%s\n%s\n%s\n\n" % (separator, title, separator)

    print >> f, "Total tasks submitted  : %d" % workflow['skeleton_tasks']
    print >> f, "Total CU translated    : %d" % len(cuds)

    for core in cfg['cores']:
        print >> f, "Total CU with %s cores : %s" % (core, cuds.count(core))

    print >> f, ''

    print >> f, "Print the first units for reference:"

    # for cud in cuds[0:4]:
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


# -----------------------------------------------------------------------------
# REPORTING
# -----------------------------------------------------------------------------
def write_email_body(cfg, run):
    '''Pass.
    '''

    substitutes = dict()

    substitutes['RUN_TAG']    = 'Run - '+run['tag']
    substitutes['SCALE']      = run['scale']
    substitutes['BINDING']    = run['binding']
    substitutes['UNIFORMITY'] = run['uniformity']
    substitutes['ITERATION']  = run['iteration']
    substitutes['RERUN']      = run['rerun']
    substitutes['RP_VERSION'] = rs.version
    substitutes['SP_VERSION'] = rp.version
    substitutes['RU_VERSION'] = ru.version

    write_template(cfg['email_template'], substitutes, run['files']['email'])

    os.system('ls -al %s >> %s' % (run['root'], run['files']['email']))

    f = open(run['files']['email'], 'r')
    email_body = f.read()
    f.close()

    return email_body


# -----------------------------------------------------------------------------
def create_diagram(run):
    '''Pass.
    '''

    enter   = 'cd %s ; ' % run['root']
    diagram = 'radicalpilot-stats -m plot,stat -s %s ' % run['session_id']
    mongodb = '-d %s ' % cfg['rp_dburl']
    fstats  = '> %s 2>/dev/null ; ' % run['files']['stats'].split('/')[-1]
    exit    = 'cd ..'

    command = enter+diagram+mongodb+fstats+exit

    os.system(command)

    run['files']['diagram'] += run['session_id']+'.png'

    return [run['files']['diagram'], run['files']['stats']]


# -----------------------------------------------------------------------------
def dump_db(cfg, run):
    '''Pass.
    '''

    dumps = []

    os.system('cd %s ; radicalpilot-close-session -m export -d %s -s %s ; cd ..' %
              (run['root'], cfg['rp_dburl'], run['session_id']))

    dumps.append(run['root']+run['session_id']+'.p.json')
    dumps.append(run['root']+run['session_id']+'.pm.json')
    dumps.append(run['root']+run['session_id']+'.um.json')
    dumps.append(run['root']+run['session_id']+'.cu.json')
    dumps.append(run['root']+run['session_id']+'.json')

    os.system('tar cfj %s %s*.json' % (run['files']['dbkp'], run['root']))

    return run['files']['dbkp']


# -----------------------------------------------------------------------------
def email_report(cfg, run):
    '''Pass.
    '''

    attachments = []

    subject = '[AIMES experiment] Run %s/-%s (%s) - %s' % \
        (run['number'], run['left'], run['tag'], run['state'])

    diagrams = create_diagram(run)

    for diagram in diagrams:
        if os.path.exists(diagram):
            attachments.append(diagram)

    attachments.append(dump_db(run))
    attachments.append(run['files']['log'])

    body = write_email_body(cfg, run)

    send_email(cfg, cfg['recipients'][0], cfg['recipients'], subject, body, attachments)


# -----------------------------------------------------------------------------
# UTILS
# -----------------------------------------------------------------------------
def uri_to_tag(resource):
    '''Pass.
    '''

    tag = {'blacklight.psc.xsede.org'  : 'xsede.blacklight',
           'gordon.sdsc.xsede.org'     : 'xsede.gordon',
           'stampede.tacc.utexas.edu'  : 'xsede.stampede',
           'stampede.tacc.xsede.org'   : 'xsede.stampede',
           'stampede.xsede.org'        : 'xsede.stampede',
           'trestles.sdsc.xsede.org'   : 'xsede.trestles',
           'hopper.nersc.gov'          : 'nersc.hopper_ccm',
           'supermic.cct-lsu.xsede.org': 'lsu.supermic'}.get(resource)

    if not tag :
        sys.exit("Unknown resource specified in bundle: %s" % resource)

    return tag


# -----------------------------------------------------------------------------
def write_template(template, substitutes, fout):
    '''Pass.
    '''

    template_fh = open(template)
    src = Template(template_fh.read())
    dest = src.substitute(substitutes)
    template_fh.close()

    skeleton = open(fout, 'w')
    skeleton.write(dest)
    skeleton.close()


# -----------------------------------------------------------------------------
def timestamp():
    '''Pass.
    '''

    # Create a time stamp for the tag to print into the radical tools file.
    now = time.time()
    tf = '%Y-%m-%d %H:%M:%S'
    stamp = datetime.datetime.fromtimestamp(now).strftime(tf)

    return stamp


# -----------------------------------------------------------------------------
def send_email(cfg, send_from, send_to, subject, body, files=None,
               server="127.0.0.1"):
    '''Pass.
    '''

    assert isinstance(send_to, list)

    msg = MIMEMultipart()

    msg['Subject'] = subject
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)

    msg.attach(MIMEText(body))

    for f in files or []:
        attachment_data = open(f, 'rb').read()
        attachment = MIMEApplication(attachment_data,
                                     name=os.path.basename(f))
        attachment.add_header('Content-Disposition',
                              'attachment; filename="%s"' % f)
        msg.attach(attachment)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


# -----------------------------------------------------------------------------
# CALLBACKS
# -----------------------------------------------------------------------------
def pilot_state_cb(pilot, state, run):
    """Called every time a ComputePilot changes its state.
    """

    # TODO: Catch an exception when a pilot fails. Check whether all the pilots
    # have failed. If so, set the run state to FAIL.

    # Mitigate the erroneous management of the pilot state from the RP
    # back-end. In some conditions, the callback is called when the state of
    # the pilot is not available even if it should be.
    if pilot:

        message = "Pilot pilot-%-13s is %-13s on %s" % (
            pilot.uid, state, pilot.resource)

        print >> run['log'], message


# -----------------------------------------------------------------------------
def unit_state_change_cb(cu, state, run):
    """Called every time a ComputeUnit changes its state.
    """

    # TODO: issue #5. Catch (and rise?) an exception when a CU fails. When
    # catching it set the state of the run to FAIL.

    # Mitigate the erroneous management of the CU state from the RP back-end.
    # In some conditions, the callback is called when the state of the CU is
    # not available even if it should be.
    if cu:

        resource = None

        for pilot in run['pilots']:
            if pilot.uid == cu.pilot_id:
                resource = pilot.resource
                break

        if not resource:

            message = "CU %-20s (unit-%s) is %s" % (cu.name, cu.uid, state)

            print >> run['log'], message

        elif not cu.pilot_id:

            message = "CU %-20s (unit-%s) is %-20s on %s" % (
                cu.name, cu.uid, state, resource)

            print >> run['log'], message

        else:

            message = "CU %-20s (unit-%s) is %-20s on %-14s (pilot-%s)" % (
                cu.name, cu.uid, state, resource, cu.pilot_id)

            print >> run['log'], message

        if state == rp.FAILED:
            print "'%s' stderr: %s." % (cu.uid, cu.stderr)
            print "'%s' stdout: %s." % (cu.uid, cu.stdout)


# -----------------------------------------------------------------------------
def wait_queue_size_cb(umgr, wait_queue_size, run):
    """Called when the size of the unit managers wait_queue changes.
    """

    message = "UnitManager (unit-manager-%s) has queue size: %s" % (
        umgr.uid, wait_queue_size)

    print >> run['log'], message


# -----------------------------------------------------------------------------
# EXECUTING
# -----------------------------------------------------------------------------
def execute_run(cfg, run):
    '''EXECUTION PATTERN: n stages, sequential:

    - Describe CU for stage 1.
    - Describe CU for stage 2.
    - ...
    - Describe CU for stage n.
    - Run Stage 1: unit input stage in; run; unit output stage out.
    - Run Stage 2: unit input stage in; run; unit output stage out.
    - ...
    - Run Stage n: unit input stage in; run; unit output stage out.
    - Shutdown.

    TODO: Currently, we leverage the knowledge we have of the skeleton - we as
    in coders. This is ad hoc and will have to be replaced by an automated
    understanding of the constraints on the execution of a specific type of
    workflow. For example, the experiment will have to learn that the type of
    Skeleton (or application) is a pipeline and will have to infer that a
    pipeline requires a sequential execution of all its stages.
    '''

    try:

        run['state'] = 'ACTIVE'

        record_run_state(run)

        # SESSION
        # -----------------------------------------------------------------
        # Create session in Radical Pilot for this run.
        session           = rp.Session(database_url=cfg['rp_dburl'])
        run['session_id'] = session.uid

        record_run_session(run)

        # WORKFLOW
        # -----------------------------------------------------------------
        # Acquire and process skeleton.
        skeleton = aimes.skeleton.Skeleton(run['files']['skeleton'])

        skeleton.generate(mode='shell')
        skeleton.setup()

        # Mine the skeleton for aggregated values.
        workflow = derive_workflow(cfg, skeleton, run)

        log_skeleton(run, workflow)

        # RESOURCES
        # ------------------------------------------------------------------
        # Acquire and process bundles.
        bundle = aimes.bundle.Bundle(query_mode=aimes.bundle.DB_QUERY,
                                     mongodb_url=cfg['bundle_dburl'],
                                     origin=cfg['bundle_origin'])

        # Mine bundles for resource properties and states.
        resources = derive_resources(cfg, bundle)

        log_bundle(run, resources)

        # STRATEGY
        # ------------------------------------------------------------------
        # Define execution strategy.
        strategy = derive_execution_stategy(cfg, workflow, resources, run)

        log_execution_stategy(cfg, run, strategy)

        # PILOT MANAGER
        # ------------------------------------------------------------------
        pmgr = rp.PilotManager(session=session)

        run['pilot_manager_id'] = pmgr.uid

        pmgr.register_callback(pilot_state_cb, callback_data=run)

        # PILOT DESCRIPTIONS
        # ------------------------------------------------------------------
        run['pdescs'] = derive_pilot_descriptions(cfg, strategy)

        log_pilot_descriptions(run)

        # CU DESCRIPTIONS
        # ------------------------------------------------------------------
        run['cuds'] = derive_cu_descriptions(run, workflow)

        log_cu_descriptions(run, workflow)

        # PILOT SUBMISSIONS
        # ------------------------------------------------------------------
        run['pilots']    = pmgr.submit_pilots(run['pdescs'])
        run['pilot_ids'] = [(p.uid, p.resource) for p in run['pilots']]

        # UNIT MANAGERS
        # ------------------------------------------------------------------
        scheduler = {'SCHED_BACKFILLING'      : rp.SCHED_BACKFILLING,
                     'SCHED_DIRECT_SUBMISSION': rp.SCHED_DIRECT_SUBMISSION
                    }[strategy['inference']['rp_scheduler']]

        umgr = rp.UnitManager(session=session, scheduler=scheduler)

        run['unit_manager_id'] = umgr.uid

        umgr.add_pilots(run['pilots'])

        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE,
                               callback_data=run)
        umgr.register_callback(unit_state_change_cb,
                               callback_data=run)

        log_rp(run)

        # EXECUTION
        # ------------------------------------------------------------------
        for stage in workflow['skeleton'].stages:

            umgr.submit_units(run['cuds'][stage.name])

            # Wait for all compute units to finish.
            umgr.wait_units()

        # DONE
        # ------------------------------------------------------------------
        run['state'] = 'DONE'

    except Exception as e:
        # this catches all RP and system exceptions
        print "Caught exception: %s" % e
        traceback.print_exc()

        run['state'] = 'FAILED'

        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), we catch the corresponding
        # KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit which gets raised if the main threads exits for
        # some other reason.
        print "Caught exception, exit now: %s" % e

        run['state'] = 'FAILED'

        raise

    finally:
        # always clean up the session, no matter whether we caught an
        # exception
        record_run_state(run)
        session.close(cleanup=False, terminate=True)

        email_report(run)


# -----------------------------------------------------------------------------
