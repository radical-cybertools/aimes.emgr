import sys
import time
import datetime

from string import Template

__author__ = "Matteo Turilli"
__copyright__ = "Copyright 2015, The AIMES Project"
__license__ = "MIT"


# -----------------------------------------------------------------------------
# UTILS
# -----------------------------------------------------------------------------
def uri_to_tag(resource):
    '''Pass.
    '''

    tag = {'localhost'                      : 'local.localhost',
           'blacklight.psc.xsede.org'       : 'xsede.blacklight',
           'gordon.sdsc.xsede.org'          : 'xsede.gordon',
           'stampede.tacc.utexas.edu'       : 'xsede.stampede',
           'stampede.tacc.xsede.org'        : 'xsede.stampede',
           'stampede.xsede.org'             : 'xsede.stampede',
           'trestles.sdsc.xsede.org'        : 'xsede.trestles',
           'hopper.nersc.gov'               : 'nersc.hopper_ccm',
           'supermic.cct-lsu.xsede.org'     : 'xsede.supermic',
           'comet.sdsc.xsede.org'           : 'xsede.comet',
           'bw.ncsa.illinois.edu'           : 'ncsa.bw',
           'xd-login.opensciencegrid.org'   : 'osg.xsede-virt-clust'}.get(resource)

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
