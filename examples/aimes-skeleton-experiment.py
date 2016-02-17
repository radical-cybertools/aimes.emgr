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
                for iteration in range(1, iterations+1):
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

    # read configuration file.
    cfg = ru.read_json(sys.argv[1])

    # TODO: Rename aimes.emgr config keys.
    cfg["skeleton_template"] = cfg["skeleton"]["template"]
    cfg["pct_concurrency"] = cfg["strategy"]["pct_concurrency"]
    cfg["pct_resources"] = cfg["strategy"]["pct_resources"]
    cfg["recipients"] = cfg["log"]["email"]["recipients"]

    # TODO: Override with json skeleton config entries.
    cfg['skeleton_task_duration'] = {
        "max": cfg["skeleton"]["tasks"]["duration"]["max"],
        "min": cfg["skeleton"]["tasks"]["duration"]["min"]}

    # Execution queue.
    queue = Queue.Queue()

    # Derive randomized run sequence and populate the default queue.
    for run_cfg in derive_run_sequence(cfg['scales'],
                                       cfg["bindings"],
                                       cfg["time_distribs"],
                                       cfg["iterations"],
                                       cfg["cores"]):
        queue.put(run_cfg)

    # Execute the queued runs.
    tracker = 0

    while not queue.empty():

        tracker += 1

        run_cfg = queue.get()

        run = aimes.emgr.create_run_environment(cfg,
                                                run_cfg,
                                                tracker,
                                                queue.qsize())
        aimes.emgr.execute_workload(cfg, run)
        aimes.emgr.finalize_run_environment(cfg, run)

        if run['state'] == 'FAILED' and run['rerun'] < cfg["reruns"]:

            # reschedule once more
            run['rerun'] += 1
            queue.put(run_cfg)
