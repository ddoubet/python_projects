#!/usr/bin/python
#Uses Python2.6 >>
#Author: Dustin Doubet
#
import sys
import time
import logging
import traceback
import cStringIO
from timeit import default_timer 
from contextlib import contextmanager
#
try:
    #Prefered library to use for linux but not required
    import subprocess32 as subprocess

except ImportError:
    import warnings
    import subprocess
    warnings.warn("subprocess32 not available as system library. "
                  "Standard library subprocess with be used")

STRING_TYPES = [str,unicode]
 
class ResourceError(Exception):
    pass

class FileIngestionError(Exception):
    pass

class FileTransferError(Exception):
    pass

class ShellCommandError(Exception):
    pass

class PathNotFoundError(Exception):
    pass


@contextmanager
def elapsed_timer():
    start = default_timer()
    elapser = lambda: default_timer() - start
    yield lambda: elapser()
    end = default_timer()
    elapser = lambda: end-start

 
def hdfs_file_transfer(transfer_type, src, dest, overwrite=False, timeout=120):
    """doc string"""
 
    if overwrite:
        force = '-f '
    else:
        force = ' '
         
    if transfer_type in ['put','copyFromLocal','get','copyToLocal']:
        shell_command = 'hdfs dfs -%s '%transfer_type + ' ' + force + ' ' + src + ' ' + dest
    else:
        raise ValueError("Invalid input for transfer_type. Valid inputs include: "
                         "[put, get, copyFromLocal, copyToLocal]")
   
    status = subprocess.call(shell_command, shell=True)
 
    if status == 0:
        status = True
    else:
        p = subprocess.Popen(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        with elapsed_timer() as elapsed:
            while True:
                if p.poll() is None:
                    time.sleep(0.5)
                else:
                    output = p.communicate()
                    if output != ('',''):
                        if output[1].lower().find('command not found') != -1:
                            raise ResourceError("HDFS not available")
                        else:
                            raise FileTransferError("Failed file transfer: %s"%output[1])
                    else:
                        status = True
                        break
 
                if elapsed() >= timeout:
                    output = p.terminate()
                    raise FileTransferError("File transfer time limit. Transfer terminated",errors=output)
 
    return status


def _run_sh_command(command, timeout):
    """doc string"""

    status = subprocess.call(command, shell=True)
 
    if status == 0:
        status = True
    else:
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        with elapsed_timer() as elapsed:
            while True:
                if p.poll() is None:
                    time.sleep(0.5)
                else:
                    output = p.communicate()
                    if output != ('',''):
                        if output[1].lower().find('command not found') != -1:
                            raise ResourceError("HDFS not available")
                        elif output[1].lower().find('no such file or directory') != -1:
                            raise PathNotFoundError("Path not found received: %s"%output[1])
                        else:
                            raise ShellCommandError("Failed to run shell command: %s, received %s"%(command,output[1]))
                    else:
                        status = True
                        break
 
                if elapsed() >= timeout:
                    output = p.terminate()
                    raise ShellCommandError("Time limit reached.  Command terminated")
 
    return status


def set_acls(path, projects, ptype, groups=None, users=None, user_prms='r-x', grp_prms='r-x', set_default=True, 
                                                                                    errors='raise', timeout=120):
    """doc string"""

    if groups is None and users is None:
        raise Exception("groups or users requires an input not None")

    status = "'No status'"

    for project in projects:
        if ptype.lower() == 'user':
            for user in users:
                aclCmd        = "hdfs dfs -setfacl -m user:%s:%s %s"%(user,user_prms,path)
                aclCmdDefault = "hdfs dfs -setfacl -m default:user:%s:%s %s"%(user,user_prms,path)
                print "Adding user: %s"%user
                for i in range(2):
                    try:
                        if set_default and i == 0:
                            status = _run_sh_command(command=aclCmdDefault, timeout=timeout)
                        elif i == 1:
                            status = _run_sh_command(command=aclCmd, timeout=timeout)

                    except Exception as e:
                        print "set acl failed for user: %s and path: %s"%(user,path)
                        print "Status: %s"%status
                        if errors == 'raise':
                            raise

        elif ptype.lower() == 'group':
            for group in groups:
                aclCmd        = "hdfs dfs -setfacl -m group:%s:%s %s"%(group,grp_prms,path)
                aclCmdDefault = "hdfs dfs -setfacl -m default:group:%s:%s %s"%(group,grp_prms,path)
                print "Adding group: %s"%group
                for i in range(2):
                    try:
                        if set_default and i == 0:
                            status = _run_sh_command(command=aclCmdDefault, timeout=timeout)
                        elif i == 1:
                            status = _run_sh_command(command=aclCmd, timeout=timeout)

                    except Exception as e:
                        print "set acl failed for group: %s and path: %s"%(group,path)
                        print "Status: %s"%status
                        if errors == 'raise':
                            raise

def main():
    paths     = sys.argv[1].split(',')
    projects  = sys.argv[2].split(',')
    users     = sys.argv[3].split(',')
    groups    = sys.argv[4].split(',')
    user_prms = sys.argv[5]
    grp_prms  = sys.argv[6]

    if grp_prms == '':
        grp_prms = user_prms


    for path in paths:
        if path not in ['','/','/data','user','home','tmp']:
            print "\nSetting acls for path: %s"%path
            set_acls(path=path, projects=projects, ptype='user', users=users, user_prms=user_prms)
            set_acls(path=path, projects=projects, ptype='group', groups=groups, grp_prms=grp_prms)
        else:
            print "Blacklisted path, skipping path: %s"%path


if __name__ == '__main__':                                
    main()








