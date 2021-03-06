#!/usr/bin/env python

import sys
import Queue
import random
import radical.utils as ru
import aimes.emgr

# -----------------------------------------------------------------------------
def derive_run_sequence(scales, bindings, uniformities, iterations, cores):
    '''Returns a randomized sequence of experimental runs.
    '''

    sequence = list()
    rerun    = '0'

    # Create the list of run parameters.
    for scale in scales:
        for binding in bindings:
            for uniformity in uniformities:
                for iteration in iterations:
                    sequence.append([scale, binding, uniformity, iteration,
                                     rerun, cores])

    # Shuffle sequence.
    random.shuffle(sequence)

    return sequence


# =============================================================================
# EXPERIMENT
# =============================================================================
if __name__ == '__main__':

    if len(sys.argv) < 2:
        print "\n\n\tusage: %s <config.json>\n\n" % sys.argv[0]
        sys.exit(-1)

    cfg = ru.read_json(sys.argv[1])


    # Experiment global constants.
  # SCALES       = [8, 16, 32, 64, 128, 256, 512, 1024, 2048]
  # SCALES         = [8, 16, 32, 64, 128, 256]
    SCALES         = [8]
    BINDINGS       = ['late']
  # CORES          = [2, 4, 8, 16]
    CORES          = [2]
    TIME_DISTRIB   = ['uniform']
  # ITERATIONS     = range(1, 5)
    ITERATIONS     = [1]
    RERUN_ATTEMPTS = 0

    cfg['scales']         = SCALES
    cfg['bindings']       = BINDINGS
    cfg['cores']          = CORES
    cfg['time_distrib']   = TIME_DISTRIB
    cfg['iterations']     = ITERATIONS
    cfg['rerun_attempts'] = RERUN_ATTEMPTS

    cfg['skeleton_task_duration'] = {'max': 1800, 'min': 60} # -- PASS
    cfg['skeleton_task_duration']['avg']   = cfg['skeleton_task_duration']['max'] / 2.0
    cfg['skeleton_task_duration']['stdev'] = cfg['skeleton_task_duration']['avg'] / 3.0

    # cfg['bundle_resources']   = {'hopper.nersc.gov'          : 'pbs',
    #                              'stampede.tacc.xsede.org'   : 'slurm'}
                                 # 'gordon.sdsc.xsede.org'     : 'pbs'}
                                 # 'blacklight.psc.xsede.org'  : 'pbs'}
                                 # 'hopper.nersc.gov'          : 'pbs'}
    cfg['bundle_resources']   = None
    cfg['bundle_unsupported'] = {'supermic.cct-lsu.xsede.org': {
                                    'sched': 'pbs',
                                    'fconf': 'bundle_resources/lsu.supermic.json'
                                }}

    cfg['remote_mongodb']     = 'mongodb://%s:%d' % (cfg['remote_mongodb_dns'],
                                                     cfg['remote_mongodb_port'])
    cfg['rp_dburl']           = '%s/%s' % (cfg['remote_mongodb'], cfg['rp_db'])
    cfg['bundle_dburl']       = '%s/%s' % (cfg['remote_mongodb'], cfg['bundle_db'])

    # Execution queue.
    queue = Queue.Queue()

    # Derive randomized run sequence and populate the default queue.
    for run_cfg in derive_run_sequence(SCALES, BINDINGS, TIME_DISTRIB, ITERATIONS,
                                       CORES):
        queue.put(run_cfg)

    # Execute the queued runs.
    tracker = 0

    while not queue.empty():

        tracker += 1

        run_cfg = queue.get()

        run = aimes.emgr.create_run_environment(cfg, run_cfg, tracker, queue.qsize())
        aimes.emgr.execute_run(cfg, run)
        aimes.emgr.finalize_run_environment(cfg, run)

        if run['state'] == 'FAILED' and run['rerun'] < RERUN_ATTEMPTS:

            # reschedule once more
            run['rerun'] += 1
            queue.put(run_cfg)


 -----------------------------------------------------------------------------

