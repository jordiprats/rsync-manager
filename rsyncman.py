#!/usr/bin/python

import os.path
import getpass
import sys
import logging
import json
import psutil,os
import datetime, time
import socket
from ConfigParser import SafeConfigParser
from subprocess import Popen,PIPE,STDOUT
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


error_count=0

def sendReportEmail(to_addr, id_host):
    global error_count
    global logFile

    from_addr=getpass.getuser()+'@'+socket.gethostname()

    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    if error_count > 0:
        msg['Subject'] = id_host+"-RSYNCMAN-OK"
    else:
        msg['Subject'] = id_host+"-RSYNCMAN-ERROR"

    body = "please check "+logFile+" on "+socket.gethostname()
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('localhost')
    text = msg.as_string()
    server.sendmail(from_addr, to_addr, text)
    server.quit()

# thank god for stackoverflow - https://stackoverflow.com/questions/25283882/determining-the-filesystem-type-from-a-path-in-python
def get_fs_type(path):
    partition = {}
    for part in psutil.disk_partitions():
        partition[part.mountpoint] = (part.fstype, part.device)
    if path in partition:
        return partition[path]
    splitpath = path.split(os.sep)
    for i in xrange(len(splitpath),0,-1):
        path = os.sep.join(splitpath[:i]) + os.sep
        if path in partition:
            return partition[path]
        path = os.sep.join(splitpath[:i])
        if path in partition:
            return partition[path]
    return ("unkown","none")

def runJob(ionice,delete,exclude,rsyncpath,path,remote,remotepath,checkfile,expected_fs):
    global error_count
    command=ionice+'rsync -v -a -H -x --numeric-ids '+delete+exclude+rsyncpath+' '+path+' '+remote+':'+remotepath
    if os.path.exists(checkfile):
        logging.info("checkfile found: "+checkfile)
        fs_type = get_fs_type(path)[0]
        if expected_fs and expected_fs != fs_type:
            logging.error("ABORTING "+path+": expected fs type does not match expected fs - found: "+fs_type+" expected: "+expected_fs)
            error_count=error_count+1
        logging.debug("RSYNC command: "+command)
        process = Popen(command,stderr=PIPE,stdout=PIPE,shell=True)
        data = process.communicate()[0]

        for line in data.splitlines():
            logging.info("RSYNC: "+line)

        if process.returncode!=0:
            logging.error("ERROR found running job for "+path)
            error_count=error_count+1
        else:
            logging.info(path+" competed successfully")
    else:
        logging.error("ABORTING "+path+": check file does NOT exists: "+checkfile)
        error_count=error_count+1

try:
    config_file = sys.argv[1]
except IndexError:
    config_file = './rsyncman.config'

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

if not os.path.isfile(config_file):
    logging.error("Error reading config file ("+config_file+")")
    sys.exit(1)

try:
    config = SafeConfigParser()
    config.read(config_file)
except Exception, e:
    logging.error("error reading config file - ABORTING - "+str(e))
    sys.exit(1)

try:
    logdir=config.get('rsyncman', 'logdir')
except:
    logdir=os.path.dirname(os.path.abspath(config_file))

logFile = "{0}/{1}-{2}.log".format(logdir, 'rsyncman', datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))
fileHandler = logging.FileHandler(logFile)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

rootLogger.setLevel(0)

try:
    to_addr=config.get('rsyncman', 'to')
except:
    to_addr=''
try:
    id_host=config.get('rsyncman', 'host-id')
except:
    id_host=socket.gethostname()

if len(config.sections()) > 0:
    for path in config.sections():
        if path != "rsyncman":
            try:
                ionice='ionice '+config.get(path, 'ionice').strip('"')+' '
            except:
                ionice=''
            try:
                expected_fs=config.get(path, 'expected-fs')
            except:
                expected_fs=''
            try:
                rsyncpath='--rsync-path="'+config.get(path, 'rsync-path').strip('"')+'"'
            except:
                rsyncpath=''
            try:
                if os.path.isabs(config.get(path, 'check-file')):
                    checkfile=config.get(path, 'check-file')
                else:
                    checkfile=path+'/'+config.get(path, 'check-file')
            except:
                checkfile=path
            try:
                if config.getboolean(path, 'delete'):
                    delete='--delete'
                else:
                    delete=''
            except:
                delete=''
            try:
                exclude_config_get = config.get(path,'exclude')
                try:
                    exclude=' '
                    for item in json.loads(exclude_config_get):
                        exclude+='--exclude '+item+' '
                except Exception, e:
                    logging.error("error reading excludes for  "+path+" - ABORTING - "+str(e))
                    error_count=error_count+1
                    continue
            except:
                exclude=' '
            try:
                remotepath=config.get(path, 'remote-path')
            except:
                remotepath=os.path.dirname(path)
            try:
                remote=config.get(path, 'remote').strip('"')

            except Exception, e:
                logging.error("remote is mandatory, aborting rsync for "+path+" - "+str(e))
                error_count=error_count+1
                continue
            runJob(ionice,delete,exclude,rsyncpath,path,remote,remotepath,checkfile,expected_fs)

            if error_count >0:
                logging.error("ERROR_COUNT:" + str(error_count))
                sys.exit(1)
            else:
                logging.info("SUCCESS")

            if to_addr:
                sendReportEmail(to_addr, id_host)

else:
    logging.error("No config found")
    sys.exit(1)
