from aimes.emgr.utils import *

__author__ = "Matteo Turilli"
__copyright__ = "Copyright 2015, The AIMES Project"
__license__ = "MIT"

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

    if 'supported' in cfg['bundle']['resources']:
        rs = cfg['bundle']['resources']['supported']

        for resource, scheduler in rs.iteritems():

            # if binding == 'early' and cfg['bundle_resources'] in resource:
            if binding == 'early':
                substitutes['RESOURCE_LIST'] = entry_template % \
                    (scheduler, resource, cfg['bundle']['username'])
                break

            substitutes['RESOURCE_LIST'] += entry_template % \
                (scheduler, resource, cfg['bundle']['username'])

    if 'unsupported' in cfg['bundle']['resources']:
        rs = cfg['bundle']['resources']['unsupported']

        for resource, properties in rs.iteritems():

            substitutes['RESOURCE_LIST'] += entry_template_unsupported % \
                (properties['sched'], resource, properties['fconf'])

    write_template(cfg['bundle']['template'], substitutes, fout)
