import random
import radical.pilot as rp


# -----------------------------------------------------------------------------
def derive_swift_workload(cfg, sw, run):
    '''We need:
        - input/output files size for each cud in sw
        - duration for each cud. If not known, we need an estimated average
          possibly set in the confif.json file.
    '''

    workload = {}

    workload['n_cus'] = len(sw['cuds'])

    # Times
    workload['t_cus']   = []
    workload['t_fins']  = []
    workload['t_fouts'] = []

    # Cores
    workload['c_cus']   = []

    for cu in sw['cuds']:
        # List of input data size.
        if 'inputs' in cu.keys():
            for i in cu['inputs']:
                workload['t_fins'].append(float(i['size']))
        else:
            workload['t_fins'].append(0.0)

        # List of output data size.
        if 'outputs' in cu.keys():
            for o in cu['outputs']:
                workload['t_fouts'].append(float(o['size']))
        else:
            workload['t_fouts'].append(0.0)

        # List of CU durations.
        if 'duration' in cu.keys():
            workload['t_cus'].append(float(cu['duration']))
        else:
            # FIXME: assumes cu['arguments'][0] to be the number of seconds
            # passed to /bin/sleep
            workload['t_cus'].append(float(cu['arguments'][0]))

        workload['c_cus'].append(cu['cores'])

    workload['tt_cus']   = sum(workload['t_cus'])
    workload['tt_fins']  = sum(workload['t_fins'])
    workload['tt_fouts'] = sum(workload['t_fouts'])

    workload['t_cus_max'] = workload['t_cus'][-1]
    workload['t_cus_min'] = workload['t_cus'][0]

    workload['tc_cus'] = sum(workload['c_cus'])

    return workload


# -----------------------------------------------------------------------------
def derive_cu_descriptions_swift(cfg, run, swift_workload):
    '''Derives CU from the given workload
    '''

    # Work around RP attribute interface.
    attributes = ['executable', 'cores', 'mpi', 'name', 'arguments',
                  'environment', 'stdout', 'stderr', 'input_staging',
                  'output_staging', 'pre_exec', 'post_exec', 'kernel',
                  'restartable', 'cleanup']

    cuds = {'all': list()}

    for cud in swift_workload['cuds']:

        if isinstance(cud, dict):
            rp_cud = rp.ComputeUnitDescription()
            for k, v in cud.items():
                if k in attributes:
                    rp_cud.set_attribute(k, v)

            cuds['all'].append(rp_cud)
        else:
            cuds['all'].append(cud)

    # Shuffle the list of CU descriptions so to minimize the impact of the
    # list ordering on the ordering of the scheduling on one or more pilots.
    random.shuffle(cuds)

    return cuds
