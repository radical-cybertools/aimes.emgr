from string import split
import math

from aimes.emgr.utils import *

# -----------------------------------------------------------------------------
def derive_execution_strategy(cfg, workload, resources, run):
    pass


# -----------------------------------------------------------------------------
def derive_execution_stategy_skeleton(cfg, workload, resources, run):
    '''Pass.
    '''

    strategy = {}
    strategy['heuristic'] = {}
    strategy['inference'] = {}

    # Degree of concurrency. Question: what amount of concurrent execution
    # minimizes TTC?
    strategy['heuristic']['percentage_concurrency'] = cfg['pct_concurrency']

    # Number of resources. Question: what is the number of resources that when
    # used to execute the tasks of the workload minimize the TTC?
    strategy['heuristic']['percentage_resources'] = cfg['pct_resources']

    # CHOOSE RESOURCES: Get the resources from the bundle.
    strategy['inference']['target_resources'] = list()

    if 'supported' in cfg['bundle']['resources']:
        for resource in cfg['bundle']['resources']['supported'].keys():

            if run['binding'] == 'early':
                strategy['inference']['target_resources'].append(
                    uri_to_tag(resource))
                break

            strategy['inference']['target_resources'].append(uri_to_tag(resource))

    if 'unsupported' in cfg['bundle']['resources']:
        for resource in cfg['bundle']['resources']['unsupported'].keys():

            if run['binding'] == 'early':
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
    #   workload to execute on a pilot of the resource overlay, given the
    #   decided degree of concurrency.
    # . Requirements: we need to be able to run all the tasks on a single
    #   pilot; i.e. the worse case scenario in which a single pilot is
    #   available for enough time that the whole workload can be run at 1/n of
    #   the optimal concurrency that would be achieved by having all the n
    #   pilots available.
    # . Implicit assumption: pilots are heterogeneous - all have the same
    #   walltime and number of cores.
    # . Formula: after sorting the length of all the tasks, the walltime
    #   accounting for the described worse case scenario is the sum of the n
    #   longest tasks with n = the number of pilots instantiated.
    strategy['inference']['compute_time_workload'] = (
        sum(workload['task_time_sorted'][
            -len(strategy['inference']['target_resources']):]))

    # - STAGING TIME: the time needed to move the I/O files of each task (that
    #   will be) bound to each pilot. We assume a conservative 5 seconds to
    #   transfer 1 MB but this value will have to be taken dynamically from a
    #   monitoring system testing the transfer speed between two given points -
    #   origin and destination.
    if (workload['skeleton_input_data'] > 0 or
        workload['skeleton_output_data'] > 0):
        strategy['inference']['staging_time_workload'] = (
            (((workload['skeleton_input_data'] +
               workload['skeleton_output_data']) / 1024) / 1024) * 5)
    else:
        strategy['inference']['staging_time_workload'] = 0

    # - RP OVERHEAD TIME: the time taken by RP to bootstrap and manage each CU
    #   for each pilot. This value needs to be assessed inferred by a
    #   performance model of RP.
    strategy['inference']['rp_overhead_time_workload'] = (
        600 + workload['skeleton_tasks'] * 4)

    # NUMBER OF CORES: Maximal concurrency is achieved by having 1 core for each
    # core needed by each task of the given workload. A minimal concurrency will
    # need to be calculated so to guarantee the availability of the minimal
    # amount of cores needed by the largest task (i.e. the tasks that need the
    # largest number of cores in order to be executed).
    strategy['inference']['cores_workload'] = math.ceil(
        (workload['stages_compute']['max'] *
         strategy['heuristic']['percentage_concurrency']) / 100.0)

    return strategy

# -----------------------------------------------------------------------------
def derive_execution_strategy_skeleton_osg(cfg, workload, resources, run):

    #   Initializing data structures
    strategy = {}
    strategy['heuristic'] = {}
    strategy['inference'] = {}

    osg_walltime = dict()
    osg_cores = dict()

    #   Defining level of concurrency (in %)
    strategy['heuristic']['percentage_concurrency'] = cfg['pct_concurrency']

    #   Defining the percentage of available resources to use
    strategy['heuristic']['percentage_resources'] = cfg['pct_resources']

    #   Initializing list of resources to use
    strategy['inference']['target_resources'] = list()

    #   Defining resources to use for resources which do and do not support bundles
    #   Also, keeping a dictionary of the walltime and number of cores requested.
    #   Appending the resource directly instead of the uri_to_tag of a resource in
    #   on the off chance that two different head nodes use the same config node
    #   Postponing the translation makes it easier to keep separate the different
    #   configurable values which the user can change (namely, cores and walltime)
    if 'supported' in cfg['bundle']['resources']:
        for resource in cfg['bundle']['resources']['supported'].keys():
            if split(uri_to_tag(resource), '.')[0] == 'osg':
                #   Checking for unique values for requested walltimes on OSG
                if resource not in osg_walltime:
                    osg_walltime[resource] = cfg['bundle']['resources']['supported'][resource]['walltime']
                #   Checking for unique values of cores requeted on OSG
                if resource not in osg_cores:
                    osg_cores[resource] = cfg['bundle']['resources']['supported'][resource]['cores']
                for i in range(cfg['bundle']['resources']['supported'][resource]['num_pilots']):
                    strategy['inference']['target_resources'].append(resource)

                    #   If early binding, then there's no point in adding more than one resources
                    if run['binding'] == 'early':
                        break

    if 'unsupported' in cfg['bundle']['resources']:
        for resource in cfg['bundle']['resources']['unsupported'].keys():
            if split(uri_to_tag(resource), '.')[0] == 'osg':
                #   Checking for unique values for requested walltimes on OSG
                if resource not in osg_walltime:
                    osg_walltime[resource] = cfg['bundle']['resources']['unsupported'][resource]['walltime']
                #   Checking for unique values of cores requeted on OSG
                if resource not in osg_cores:
                    osg_cores[resource] = cfg['bundle']['resources']['unsupported'][resource]['cores']
                for i in range(cfg['bundle']['resources']['unsupported'][resource]['num_pilots']):
                    strategy['inference']['target_resources'].append(resource)

                    #   If early binding, then there's no point in adding more than one resources
                    if run['binding'] == 'early':
                        break


    #   Choosing the Number of pilots, assuming max concurrency. Casting the number
    #   OSG pilots to an int is to ensure that the value of the pilots specified
    #   is an int. Assertion that the number of pilots submitted is also to ensure
    #   the user will submit a reasonable number of pilots
    if strategy['heuristic']['percentage_resources'] == 100:
        strategy['inference']['number_pilots'] = len(strategy['inference']['target_resources'])
        assert strategy['inference']['number_pilots'] >= 1, "Must specify a positive integer for the number of OSG pilots to submit\n"
            
    #   Choosing the scheduler for the CUs. If there is only one pilot. then
    #   submit directly to the pilot. If there is more than one pilot. The number
    #   of pilots has been asserted to be a strictly positive integer
    if strategy['inference']['number_pilots'] > 1:
        strategy['inference']['rp_scheduler'] = 'SCHED_BACKFILLING'
    else:
        strategy['inference']['rp_scheduler'] = 'SCHED_ROUND_ROBIN'

    #   On OSG, it is not necessary to submit a wall time. However, different to provide
    #   the functionality to submit to OSG via different login/head nodes with different
    #   walltimes, we feed a dictionary of the walltimes which will be parsed in the
    #   when deriving pilot descriptions
    strategy['inference']['compute_time_workload'] = osg_walltime
    

    #   Staging Time, the needed to move the I/O files of each task bound to each
    #   pilot. We assume conservatively 5 sec per 1 MB, but this value will be taken
    #   dynamically from a monitoring system testing the transfer speed between the
    #   origin and destination

    if (workload['skeleton_input_data'] > 0 or workload['skeleton_output_data'] > 0):
        strategy['inference']['staging_time_workload'] = \
            (workload['skeleton_input_data'] + workload['skeleton_output_data']) / (1024 * 1024 * 5)
    else:
        strategy['inference']['staging_time_workload'] = 0

    #   RP Overhead, the time taken by RP to bootstrap and manage each CU for each
    #   pilot. This value needs to be assessed inferred by a performance model of RP
    strategy['inference']['rp_overhead_time_workload'] = 600 + (workload['skeleton_tasks'] * 4)

    #   Number of cores. On OSG, we can request any number of cores, though 1 is the
    #   most typical value. To allow for the capability to request different numbers
    #   of cores at different login/head nodes of OSG, we feed a dictionary of number
    #   of cores which will be requested.
    strategy['inference']['cores_workload'] = osg_cores

    
    return strategy
    
# -----------------------------------------------------------------------------
def derive_execution_stategy_swift(cfg, sw, resources, run):
    '''
    cfg = configuration file
    sw  = swift workload

    the returned strategy needs to contain:

        strategy['inference']['target_resources']
        strategy['inference']['cores_workload']
        strategy['inference']['number_pilots']
        strategy['inference']['compute_time_workload']
        strategy['inference']['staging_time_workload']
        strategy['inference']['rp_overhead_time_workload']
    '''

    # CHOOSE RESOURCES: Get the resources from the bundle.
    target_resources = list()

    if 'supported' in cfg['bundle']['resources']:
        for resource in cfg['bundle']['resources']['supported'].keys():

            if run['binding'] == 'early':
                target_resources.append(uri_to_tag(resource))
                break

            target_resources.append(uri_to_tag(resource))

    if 'unsupported' in cfg['bundle']['resources']:
        for resource in cfg['bundle']['resources']['unsupported'].keys():

            if run['binding'] == 'early':
                target_resources.append(uri_to_tag(resource))
                break

            target_resources.append(uri_to_tag(resource))

    # CHOOSE NUMBER OF PILOTS: Adopt an heuristics that tells us how many
    # concurrent resources we should choose given the execution time boundaries.
    # We assume that task concurrency should always be maximized we may decide
    # that we want to start with #pilots = #resources to which we have access.
    if cfg['strategy']['pct_resources'] == 100:
        number_pilots = len(target_resources)

    # CHOOSE THE SCHEDULER FOR THE CUs: Depending on whether we have multiple
    # pilot and on what metric needs to bo min/maximized. In this demo we
    # minimize TTC so we choose backfilling. Do we have a default scheduler? If
    # so, an else is superfluous.
    if len(target_resources) > 1:
        rp_scheduler = 'SCHED_BACKFILLING'
    else:
        rp_scheduler = 'SCHED_DIRECT_SUBMISSION'

    # TIME COMPONENTS OF EACH PILOT WALLTIME:
    #
    # - COMPUTE TIME: the time taken by the tasks to execute on a pilot of the
    #   resource overlay, given the decided degree of concurrency.
    # . Requirements: run all the tasks on a single pilot. Worse case scenario:
    #   a single pilot is available for enough time that all the tasks can
    #   execute at 1/n_pilots of the optimal concurrency that would be achieved
    #   by having all the n_pilots available.
    # . Implicit assumption: pilots are heterogeneous - all have the same
    #   walltime and number of cores.
    # . Formula: after sorting the length of all the tasks, the walltime
    #   accounting for the described worse case scenario is the sum of the n
    #   longest tasks with n = the number of pilots instantiated.
    sw['t_cus_sorted'] = sorted(sw['t_cus'])
    compute_time = sum(sw['t_cus_sorted'][-len(target_resources):])

    # - STAGING TIME: the time needed to move the I/O files of each task (that
    #   will be) bound to each pilot. We assume a conservative 5 seconds to
    #   transfer 1 MB but this value will have to be taken dynamically from a
    #   monitoring system testing the transfer speed between two given points -
    #   origin and destination.
    staging_time = (((sw['tt_fins'] + sw['tt_fouts']) / 1024) / 1024) * 5

    # - RP OVERHEAD TIME: the time taken by RP to bootstrap and manage each CU
    #   for each pilot. This value needs to be assessed inferred by a
    #   performance model of RP.
    # rp_overhead_time = 600 + sw['n_cus'] * 4
    rp_overhead_time = 900 + sw['n_cus'] * 4

    # NUMBER OF CORES: Maximal concurrency is achieved by having 1 core for each
    # core needed by each task of the given workload. A minimal concurrency will
    # need to be calculated so to guarantee the availability of the minimal
    # amount of cores needed by the largest task (i.e. the tasks that need the
    # largest number of cores in order to be executed).
    compute_cores = math.ceil((sw['tc_cus'] * cfg['strategy']['pct_concurrency']) / 100.0)


    # TODO: Clean this up.
    info = {'target_resources'          : target_resources,
            'cores_workload'            : compute_cores,
            'number_pilots'             : number_pilots,
            'compute_time_workload'     : compute_time,
            'staging_time_workload'     : staging_time,
            'rp_overhead_time_workload' : rp_overhead_time,
            'percentage_concurrency'    : cfg['strategy']['pct_concurrency'],
            'percentage_resources'      : cfg['strategy']['pct_resources'],
            'rp_scheduler'              : rp_scheduler
            }

    strategy = {'heuristic' : info,
                'inference' : info}

    return strategy
