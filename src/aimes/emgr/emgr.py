
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
import math
import random
import logging
import traceback

import radical.pilot as rp

import aimes.bundle
import aimes.skeleton

from aimes.emgr.utils import *
from aimes.emgr.strategy import *
from aimes.emgr.workloads import *
from aimes.emgr.resources import *

__author__ = "Matteo Turilli"
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
                    'runtime' : cfg['log']['file'],
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
    if cfg['workload_type'] == "skeleton":
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
            cfg['bundle']['origin'], 'in')

        resources['bandwidth_out'][resource.name] = resource.get_bandwidth(
            cfg['bundle']['origin'], 'out')

    # Get the total core capacity offered by the default queues of the target
    # resources.
    resources['core_capacity'] = 0

    for r_name in bundle.resources:
            resource = bundle.resources[r_name]

            for q_name in resource.queues:
                queue = resource.queues[q_name]

                if (q_name == 'normal' or q_name == 'batch' or
                    q_name == 'default' or q_name == 'regular'):
                        resources['core_capacity'] += queue.num_procs_limit

    # Resource IDs.
    resources['resource_ids'] = [
        str(bundle.resources[resource].name) for resource in bundle.resources]

    resources['bundle'] = bundle

    return resources


# -----------------------------------------------------------------------------
def derive_pilot_descriptions(cfg, strategy):
    '''Pass.
    '''

    pdescs = []

    # FIXME. Create a pilot description for each resource. Assumption: there is
    # a single pilot for each resource and the number of pilots is the same of
    # the number of resources.
    for resource in strategy['inference']['target_resources']:

        # if not resource in cfg['resources']:
        #     print "ERROR: No configuration for resource %s." % resource
        #     sys.exit(1)

        pdesc = rp.ComputePilotDescription()

        pdesc.project  = cfg['project_ids'].get(resource)
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
def derive_cu_descriptions(cfg, run, workflow):
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
            # cud.restartable = True
            cud.cleanup = True

            cuds[stage.name].append(cud)

    # Shuffle the list of CU descriptions so to minimize the impact of the
    # list ordering on the ordering of the scheduling on one or more pilots.
    random.shuffle(cuds)

    return cuds


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
def execute_workload(cfg, run):
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
        session           = rp.Session(database_url=cfg['mongodb'])
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
        # Acquire and process bundles. Connect to bundle DB only if config file
        # sets supported resources.
        if 'supported' in cfg['bundle']['resources']:
            bundle = aimes.bundle.Bundle(query_mode=aimes.bundle.DB_QUERY,
                                         mongodb_url=cfg['bundle']['mongodb'],
                                         origin=cfg['bundle']['origin'])

            # Mine bundles for resource properties and states.
            resources = derive_resources(cfg, bundle)

            log_bundle(run, resources)
        else:
            # No need to derive info for unsupported resources.
            resources = {}

        # STRATEGY
        # ------------------------------------------------------------------
        # Define execution strategy.
        strategy = derive_execution_stategy_skeleton(cfg, workflow, resources, run)

        log_execution_stategy(cfg, run, strategy)

        # PILOT MANAGER
        # ------------------------------------------------------------------
        pmgr = rp.PilotManager(session=session)

        run['pilot_manager_id'] = pmgr.uid

        pmgr.register_callback(pilot_state_cb, cb_data=run)

        # PILOT DESCRIPTIONS
        # ------------------------------------------------------------------
        run['pdescs'] = derive_pilot_descriptions(cfg, strategy)

        log_pilot_descriptions(run)

        # CU DESCRIPTIONS
        # ------------------------------------------------------------------
        run['cuds'] = derive_cu_descriptions(cfg, run, workflow)

        log_cu_descriptions(cfg, run, workflow)

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
                               cb_data=run)
        umgr.register_callback(unit_state_change_cb,
                               cb_data=run)

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

        email_report(cfg, run)


# -----------------------------------------------------------------------------
#
# same for a swift workload
#
def execute_swift_workload(cfg, run, swift_workload, swift_cb=None):
    '''TODO
    '''

    # print 'execute swift workload'
    import pprint
    # pprint.pprint(cfg)
    # pprint.pprint(run)
    pprint.pprint(swift_workload)
    # return 'wohoo!'

    session = None
    sid     = None

    try:

        run['state'] = 'ACTIVE'

        record_run_state(run)

        # SESSION
        # -----------------------------------------------------------------
        # Create session in Radical Pilot for this run.
        session           = rp.Session(database_url=cfg['mongodb'])
        run['session_id'] = session.uid

        record_run_session(run)

        # RESOURCES
        # ------------------------------------------------------------------
        # Acquire and process bundles. Connect to bundle DB only if config file
        # sets supported resources.
        bundle = None

        if 'supported' in cfg['bundle']['resources']:
            bundle = aimes.bundle.Bundle(query_mode=aimes.bundle.DB_QUERY,
                                         mongodb_url=cfg['bundle']['mongodb'],
                                         origin=cfg['bundle']['origin'])

            # Mine bundles for resource properties and states.
            resources = derive_resources(cfg, bundle)

            log_bundle(run, resources)

        else:
            # No need to derive info for unsupported resources.
            resources = {}

        # WORKLOAD
        # ------------------------------------------------------------------
        # Derive workload for the execution strategy.
        sw = derive_swift_workload(cfg, swift_workload, run)

        pprint.pprint(sw)

        # STRATEGY
        # ------------------------------------------------------------------
        # Define execution strategy.
        strategy = derive_execution_stategy_swift(cfg, sw, resources, run)

        log_execution_stategy(cfg, run, strategy)

        # PILOT MANAGER
        # ------------------------------------------------------------------
        pmgr = rp.PilotManager(session=session)

        run['pilot_manager_id'] = pmgr.uid

        pmgr.register_callback(pilot_state_cb, cb_data=run)

        # PILOT DESCRIPTIONS
        # ------------------------------------------------------------------
        run['pdescs'] = derive_pilot_descriptions(cfg, strategy)

        log_pilot_descriptions(run)

        # CU DESCRIPTIONS
        # ------------------------------------------------------------------
        run['cuds'] = derive_cu_descriptions_swift(cfg, run, swift_workload)

        log_cu_descriptions(cfg, run, swift_workload)

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
                               cb_data=run)
        umgr.register_callback(unit_state_change_cb,
                               cb_data=run)

        if swift_cb:
            umgr.register_callback(swift_cb)

        log_rp(run)

        # EXECUTION
        # ------------------------------------------------------------------
        umgr.submit_units(run['cuds']['all'])

        # Wait for all compute units to finish.
        umgr.wait_units()

        # DONE
        # ------------------------------------------------------------------
        run['state'] = 'DONE'

    except Exception as e:
        # this catches all RP and system exceptions
        m = "swift workload execution failed: %s" % e
        logging.exception(m)
        run['state'] = 'FAILED'
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), we catch the corresponding
        # KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit which gets raised if the main threads exits for
        # some other reason.
        m = "swift workload execution aborted: %s" % e
        logging.exception(m)
        run['state'] = 'FAILED'
        raise

    finally:
        # always clean up the session, no matter whether we caught an
        # exception
        record_run_state(run)
        sid = session.uid
        if session:
            session.close(cleanup=False, terminate=True)

        if 'email' in cfg['log']['media']:
            email_report(cfg, run)

    return sid
