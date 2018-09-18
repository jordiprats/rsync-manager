#!/usr/bin/python

import os.path
import sys
import logging
import json
import psutil,os
import datetime, time
from ConfigParser import SafeConfigParser
from subprocess import Popen,PIPE,STDOUT

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
    command=ionice+'rsync -v -a -H -x --numeric-ids '+delete+exclude+rsyncpath+' '+path+' '+remote+':'+remotepath
    if os.path.exists(checkfile):
        logging.info("checkfile found: "+checkfile)
        fs_type = get_fs_type(path)[0]
        if expected_fs and expected_fs != fs_type:
            logging.error("ABORTING "+path+": expected fs type does not match expected fs - found: "+fs_type+" expected: "+expected_fs)
        logging.debug("RSYNC command: "+command)
        process = Popen(command,stderr=PIPE,stdout=PIPE,shell=True)
        data = process.communicate()[0]

        for line in data.splitlines():
            logging.info("RSYNC: "+line)

        if process.returncode!=0:
            logging.error("ERROR found running job for "+path)
        else:
            logging.info(path+" competed successfully")
    else:
        logging.error("ABORTING "+path+": check file does NOT exists: "+checkfile)

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

logFile = "{0}/{1}-{2}.log".format(os.path.dirname(os.path.abspath(config_file)), 'rsyncman', datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S'))
fileHandler = logging.FileHandler(logFile)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

try:
    config = SafeConfigParser()
    config.read(config_file)
except Exception, e:
    logging.error("error reading config file - ABORTING - "+str(e))
    sys.exit(1)

rootLogger.setLevel(0)

if len(config.sections()) > 0:
    for path in config.sections():
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
            continue
        runJob(ionice,delete,exclude,rsyncpath,path,remote,remotepath,checkfile,expected_fs)

else:
    logging.error("No config found")
    sys.exit(1)
