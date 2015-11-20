import os
import smtplib

import saga as rs
import radical.pilot as rp
import radical.utils as ru

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from aimes.emgr.utils.misc import *

__author__ = "Matteo Turilli"
__copyright__ = "Copyright 2015, The AIMES Project"
__license__ = "MIT"


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

    write_template(cfg['log']['email']['template'], substitutes,
                       run['files']['email'])

    os.system('ls -al %s >> %s' % (run['root'], run['files']['email']))

    f = open(run['files']['email'], 'r')
    email_body = f.read()
    f.close()

    return email_body


# -----------------------------------------------------------------------------
def create_diagram(cfg, run):
    '''Pass.
    '''

    enter   = 'cd %s ; ' % run['root']
    diagram = 'radicalpilot-stats -m plot,stat -s %s ' % run['session_id']
    mongodb = '-d %s ' % cfg['mongodb']
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
              (run['root'], cfg['mongodb'], run['session_id']))

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

    # only go thrtough the hazzle of SMTP setup if we have anything to send in
    # the first place
    if not cfg['recipients']:
        return

    attachments = []

    subject = '[AIMES experiment] Run %s/-%s (%s) - %s' % \
        (run['number'], run['left'], run['tag'], run['state'])

    diagrams = create_diagram(cfg, run)

    for diagram in diagrams:
        if os.path.exists(diagram):
            attachments.append(diagram)

    attachments.append(dump_db(cfg, run))
    attachments.append(run['files']['log'])

    body = write_email_body(cfg, run)

    send_email(cfg, cfg['log']['email']['recipients'][0],
               cfg['log']['email']['recipients'],
               subject, body, attachments)


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
