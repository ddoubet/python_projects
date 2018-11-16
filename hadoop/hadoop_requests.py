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
import requests
import pywebhdfs
from pywebhdfs.webhdfs import PyWebHdfsClient

STRING_TYPES = [str,unicode]
 
class ResourceError(Exception):
    pass

class FileIngestionError(Exception):
    pass

class FileTransferError(Exception):
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
     
    try:
        #Prefered library to use for linux but not required
        import subprocess32 as subprocess
 
    except ImportError:
        import warnings
        import subprocess
        warnings.warn("subprocess32 not available as system library. "
                      "Standard library subprocess with be used")
 
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
 
 
class HadoopHTTPRequests:
     
    def __init__(self, oozie_url=None, web_hdfs_host=None, web_hdfs_port=None, username=None,
                       job_tracker=None, name_node=None, timeout=5, wf_logger=None, log_to_file=True, 
                                                                      log_level=None, hdlr_path=None):
         
        """doc string"""
         
        if wf_logger == None:
            self.loggerName = 'HadoopHTTPRequests'
            if log_to_file:
                self.logger = logging.getLogger(self.loggerName)
                if hdlr_path is None:
                    hdlr = logging.FileHandler(self.loggerName + '.log')
                else:
                    hdlr = logging.FileHandler(hdlr_path)
                formatter = logging.Formatter(
                        "%(asctime)s    [%(name)s]   %(levelname)s    %(message)s")
                hdlr.setFormatter(formatter)
                self.logger.addHandler(hdlr)
                if log_level is None:
                    self.logger.setLevel(logging.INFO)
                else:
                    self.logger.setLevel(log_level.upper())
            else:
                self.logger = logging.getLogger(self.loggerName)
                hdlr = logging.StreamHandler()
                formatter = logging.Formatter(
                        '%(asctime)s    %(name)s    %(levelname)s    %(message)s')
                hdlr.setFormatter(formatter)
                self.logger.addHandler(hdlr)
                if log_level is None:
                    self.logger.setLevel(logging.INFO)
                else:
                    self.logger.setLevel(log_level.upper())
        else:
            self.loggerName = wf_logger 
            self.logger = logging.getLogger(self.loggerName)
             
        #/////////////////////////////////////////////////////////////////////////////
         
        self.RETRIES      = 3
        self.username     = username
        self.nameNode     = name_node
        self.timeout      = timeout
        self.jobTracker   = job_tracker
        self.webHDFSHost  = web_hdfs_host
        self.webHDFSPort  = web_hdfs_port
        self.oozieBaseUrl = oozie_url
         
        self.Client = self.webhdfs_client(web_hdfs_host=self.webHDFSHost, 
                                          web_hdfs_port=self.webHDFSPort, 
                                          username=self.username, 
                                          timeout=self.timeout)
         
        #/////////////////////////////////////////////////////////////////////////////
             
       
    def webhdfs_client(self, web_hdfs_host=None, web_hdfs_port=None, username=None, timeout=5):
        """doc string"""
         
        self.logger.debug("In method, webhdfs_client")
        self.logger.info("Establishing WebHDFS client connection")
         
        if web_hdfs_host is None:
            web_hdfs_host = self.webHDFSHost
            web_hdfs_port = self.webHDFSPort
            username      = self.username
             
             
        self.logger.debug("PyWebHdfsClient(host=%s, port=%s,user_name=%s)"
                          %(web_hdfs_host,web_hdfs_port,username))
     
        #Cluster could be overloaded with connections and it might take a few tries
        try:
            client = PyWebHdfsClient(host=web_hdfs_host,port=web_hdfs_port, 
                                        user_name=username, timeout=timeout)
 
        except Exception as e:
            self.logger.debug("In method, webhdfs_client")
            self.logger.error("Received '%s' while creating WebHDFS client class instance"%e,
                              exc_info=True)
            raise
        else:
            self.logger.debug("In method, webhdfs_client")
            self.logger.debug("WebHDFS client connection successfully created")
         
        return client
     
     
    def hdfs_create_file(self, filename, file_content, hdfs_file_path, overwrite=False, 
                                                                client=None, **kwargs):
        """doc string"""
         
        self.logger.debug("In method, hdfs_create_file")
         
        if client is None:
            client = self.Client
             
        self.logger.info("Creating file at HDFS location '%s'"%hdfs_file_path)
         
        try:
            client.create_file(path=hdfs_file_path.lstrip('/'), file_data=file_content, 
                                                         overwrite=overwrite, **kwargs)
 
        except Exception as e:
            self.logger.debug("In method, hdfs_create_file")
            self.logger.error("Received '%s' while transfering file '%s' to HDFS location "
                              "'%s'"%(e,filename,hdfs_file_path),exc_info=True)
 
            if isinstance(e, IOError):
                traceback.print_exc(file=sys.stderr)
                #ConnectionError, ConnectionRefusedError,...etc. Need to confirm that all
                #connection type errors are an instance of IOError
                raise ResourceError("WebHDFS error during create_file. File '%s' was not "
                                    "transfered to HDFS"%filename)
            else:
                raise
        else:
            self.logger.debug("In method, hdfs_create_file")
            self.logger.debug("File successfully created at HDFS location '%s'"
                              %hdfs_file_path)
                 
        return
     
     
    def hdfs_remove(self, path, recursive=False, client=None, **kwargs):
        """doc string"""
         
        self.logger.debug("In method, hdfs_remove")
         
        if client is None:
            client = self.Client
             
        self.logger.info("Submitting request for HDFS path '%s' removal"%path)
         
        for i in range(self.RETRIES):
            try:
                client.delete_file_dir(path=path.lstrip('/'), recursive=recursive)
 
            except Exception as e:
                self.logger.debug("In method, hdfs_remove")
                self.logger.error("Received '%s' while removing HDFS path "
                                  "'%s'"%(e,path),exc_info=True)
                 
                if i+1 != self.RETRIES:
                    if isinstance(e, IOError):
                        traceback.print_exc(file=sys.stderr)
                        self.logger.warning("Resubmitting request for HDFS path removal")
                        time.sleep(i*3)
                    else:
                        raise
                     
                elif isinstance(e, IOError):
                    traceback.print_exc(file=sys.stderr)
                    #ConnectionError, ConnectionRefusedError,...etc. 
                    raise ResourceError("WebHDFS error during delete_file_dir. HDFS path '%s' was not "
                                        "removed"%path)
                else:
                    raise
            else:
                self.logger.debug("In method, hdfs_remove")
                self.logger.debug("HDFS path '%s' successfully removed"%path)
                break
                 
        return
     
     
    def hdfs_mkdir(self, hdfs_dir, permission, client=None, **kwargs):
        """doc string"""
         
        self.logger.debug("In method, hdfs_mkdir")
         
        if client is None:
            client = self.Client
             
        directory = hdfs_dir
             
        self.logger.info("Creating directory at HDFS location '%s'"%directory)
         
        for i in range(self.RETRIES):
            try:
                client.make_dir(directory.lstrip('/'), permission=permission, **kwargs)
 
            except Exception as e:
                self.logger.debug("In method, hdfs_mkdir")
                self.logger.error("Received '%s' while creating directory at HDFS location "
                                  "'%s'"%(e,directory),exc_info=True)
                 
                if i+1 != self.RETRIES:
                    if isinstance(e, IOError):
                        traceback.print_exc(file=sys.stderr)
                        self.logger.warning("Resubmitting request for directory creation")
                        time.sleep(i*3)
                    else:
                        raise
                     
                elif isinstance(e, IOError):
                    traceback.print_exc(file=sys.stderr)
                    #ConnectionError, ConnectionRefusedError,...etc. Need to confirm that all
                    #connection type errors are an instance of IOError
                    raise ResourceError("WebHDFS error during make_dir. Directory '%s' was not "
                                        "created in HDFS"%directory)
                else:
                    raise
            else:
                self.logger.debug("In method, hdfs_mkdir")
                self.logger.debug("Directory '%s' successfully created in HDFS"%directory)
                break
                 
        return
     
     
    def hdfs_listdir(self, hdfs_dir, entry_type='all', client=None, **kwargs):
        """List a directory in HDFS
             
            Inputs:
                 
                 
                entries:   str/unicode, Defaults to 'all', can
                           be 'dir' and 'file' for the types of 
                           directory entries to return.
         
        """
         
        self.logger.debug("In method, hdfs_listdir")
         
        if client is None:
            client = self.Client
             
        directory  = hdfs_dir
        entry_type = entry_type.lower()
        self.logger.debug("Listing '%s' entry types at HDFS location '%s'"
                         %(entry_type,directory))
         
        for i in range(self.RETRIES):
            try:
                #The lstrip is required for pywebhdfs- Ex: home/data/
                status = client.list_dir(directory.lstrip('/'))
 
            except Exception as e:
                self.logger.debug("In method, hdfs_listdir")
                self.logger.error("Received '%s' while listing directory for '%s'"
                                  %(e,directory),exc_info=True)
 
                if i+1 != self.RETRIES:
                    if isinstance(e, IOError):
                        traceback.print_exc(file=sys.stderr)
                        self.logger.warning("Resubmitting request for directory listing")
                        time.sleep(i*3)
                    else:
                        raise
                         
                elif isinstance(e, IOError):
                    traceback.print_exc(file=sys.stderr)
                    #ConnectionError, ConnectionRefusedError,...etc. Need to confirm that all
                    #connection type errors are an instance of IOError
                    raise ResourceError("WebHDFS error during list_dir on path '%s'"%directory) 
                else:
                    raise
            else:
                self.logger.debug("In method, hdfs_listdir")
                self.logger.debug("Successful HDFS directory listing")
                if entry_type == 'all':
                    hdfsDirList = [item['pathSuffix'] for item in status['FileStatuses']['FileStatus']]
 
                elif entry_type == 'file':
                    hdfsDirList = [item['pathSuffix'] for item in status['FileStatuses']['FileStatus'] 
                                   if item['type'] == 'FILE']
 
                elif entry_type == 'dir':    
                    hdfsDirList = [item['pathSuffix'] for item in status['FileStatuses']['FileStatus'] 
                                   if item['type'] == 'DIRECTORY']
                break
                 
        return hdfsDirList
     
     
    def hdfs_isfile(self, path, client=None):
        """doc string"""
         
        self.logger.debug("In method, hdfs_isfile")
         
        if client is None:
            client = self.Client
             
        isfile = False
        try:
            status = self.hdfs_status(file_dir=path, client=client)
             
        except pywebhdfs.errors.PyWebHdfsException as e:
            self.logger.debug("In method, hdfs_isfile")
 
            if type(e) == pywebhdfs.errors.FileNotFound:
                 pass
            else:
                raise
        else:
            self.logger.debug("In method, hdfs_isfile")
            if status['FileStatus']['type'] == 'FILE':
                isfile = True
                 
        return isfile
     
     
    def hdfs_isdir(self, path, client=None):
        """doc string"""
         
        self.logger.debug("In method, hdfs_isdir")
         
        if client is None:
            client = self.Client
         
        isdir = False
        try:
            status = self.hdfs_status(file_dir=path, client=client)
             
        except pywebhdfs.errors.PyWebHdfsException as e:
            self.logger.debug("In method, hdfs_isdir")
 
            if type(e) == pywebhdfs.errors.FileNotFound:
                 pass
            else:
                raise e
        else:
            self.logger.debug("In method, hdfs_isdir")
            if status['FileStatus']['type'] == 'DIRECTORY':
                isdir = True
                 
        return isdir
     
     
    def hdfs_status(self, file_dir, client=None):
        """doc string"""
         
        self.logger.debug("In method, hdfs_status")
         
        if client is None:
            client = self.Client
             
        directory = file_dir
         
        for i in range(self.RETRIES):
            try:
                status = client.get_file_dir_status(file_dir.lstrip('/'))
 
            except Exception as e:
                self.logger.debug("In method, hdfs_status")
                self.logger.error("Received '%s' while requesting status for '%s'"
                                  %(e,file_dir))
                if i+1 != self.RETRIES:
                    if isinstance(e, IOError):
                        traceback.print_exc(file=sys.stderr)
                        self.logger.warning("Resubmitting request for file/directory status")
                        time.sleep(i*3)
                    else:
                        raise e
                     
                elif isinstance(e, IOError):
                    traceback.print_exc(file=sys.stderr)
                    #ConnectionError, ConnectionRefusedError,...etc.
                    raise ResourceError("WebHDFS error during get_file_dir_status")  
                else:
                    raise e
            else:
                self.logger.debug("In method, hdfs_status")
                self.logger.debug("HDFS file/directory status retrieved successfully")
                break
                 
        return status
     
     
     
    def hdfs_file_from_frame(self, frame, hdfs_dir, filename, file_info=None, db_trans_funcs=None, 
                                                             client=None, dt_format=None, **kwargs):
        """doc string"""
         
        self.logger.debug("In method, hdfs_file_from_frame")
         
        if client is None:
            client = self.Client
             
        fileInfo = file_info
        dataframe = frame
         
        hdfsFilePath = hdfs_dir.rstrip('/') + '/' + filename
         
        self.logger.info("Verifying if file already exists")
        if self.hdfs_isfile(path=hdfsFilePath):
            raise FileTransferError("Failed WebHDFS file transfer. File '%s' already exists "
                                    "in location '%s'"%(filename,hdfs_dir))
        else:
            self.logger.info("File does not already exists. Proceeding with file transfer")
         
        success = False
        try:
            memoryFile = cStringIO.StringIO()
            #Write file as csv to a buffer
            self.logger.debug("Writting file to string buffer...")
            dataframe.to_csv(memoryFile, header=True, index=True, index_label='index', 
                                                                date_format=dt_format)
 
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            self.logger.error("Received '%s' while writting file to string buffer"%e,
                               exc_info=True)
 
            memoryFile.close()
            if type(e) in (UnicodeEncodeError, UnicodeDecodeError):
                import StringIO
                self.logger.info("Trying Python StringIO buffer inferface for better "
                                 "unicode handling")
                 
                memoryFile = StringIO.StringIO()
                dataframe.to_csv(memoryFile, header=True, index=True, index_label='index', 
                                                   encoding='utf-8', date_format=dt_format)
                success = True
        else:
            self.logger.debug("File successfully written to string buffer")
            success = True
        finally:
            if not success:
                self.logger.error("Received '%s' while writing file to string buffer"%e)
                memoryFile.close()
                #Raise error which will quarantine file
                raise FileTransferError("Failed to write file contents to memory buffer")
            else:
                fileContent = memoryFile.getvalue()
                 
                #When using with a database and keeping up with file transfer as a transaction
                if db_trans_funcs is not None:
                    db_connect = db_trans_funcs['db_connect']
                    file_location_status = db_trans_funcs['file_location_status']
                    #Create transaction to update file status and location upon file transfer
                    log_trans, log_conn = db_connect(instance='LOG', transaction=True)
                    fileInfo = file_location_status(hdfsFilePath=hdfsFilePath, fileInfo=fileInfo, 
                                                                                conn=log_conn)
                else:
                    #Stand alone use only
                    log_trans, log_conn = None, None
                     
                for i in range(self.RETRIES):
                    try:
                        #Might have had an issue with a connection on first try so try again
                        self.hdfs_create_file(filename=filename, file_content=fileContent, 
                                              hdfs_file_path=hdfsFilePath, client=client, **kwargs)
 
                    except ResourceError:
                        #Errors and traceback are logged in the hdfs_create_file method
                        self.logger.debug("In method, hdfs_file_from_frame")
                        if i+1 != self.RETRIES:
                            self.logger.info("Resubmitting request for WebHDFS file transfer")
                            time.sleep(3*i)
                        else:
                            if log_trans is not None:
                                log_trans.rollback()
                                self.logger.warning("Rolling back any inserts or updates to FILE_STATUS "
                                                    "and FILE_LOCATION tables")
                            #Issue remains so re-raise the ResourceError
                            raise
                        #Possible file content problem. Will have to test for other failure cases. 
                        #Will have to verify condition for when a file already exists in HDFS location.
                        #Other errors: Maybe memory full error. Might also consider trying to transfer 
                        #file in chucks as a backup method before raising error.
                    except Exception:
                        #Errors and traceback are logged in the hdfs_create_file method
                        if log_trans is not None:
                            log_trans.rollback()
                            self.logger.warning("Rolling back any inserts or updates to FILE_STATUS "
                                                "and FILE_LOCATION tables")
                        raise   
                    else:
                        if log_trans is not None:
                            log_trans.commit()
                        self.logger.debug("In method, hdfs_file_from_frame")
                        self.logger.info("File '%s' successfully transfered to HDFS"%filename)
                        break
                         
                    finally:
                        if log_conn is not None and not log_conn.closed:
                            self.logger.info("Closing log database connection")
                            log_conn.close()
                         
        return hdfsFilePath, fileInfo
 
     
    def oozie_commander(self, model, serial, job_id, file_category, oozie_url=None, 
                              hdfs_file_path=None, start_job=False, re_run=False, get_status=False, 
                              get_logs=False, get_version=False, get_object=False, oozie_id=None, 
                              config_xml=None, username=None, name_node=None, job_tracker=None, **kwargs):
        """doc string"""
         
        self.logger.debug("In method, oozie_commander")
         
        if oozie_url is None:
            oozieHttpUrl = self.oozieBaseUrl
        else:
            oozieHttpUrl = oozie_url
             
        if job_tracker is None:
            username    = self.username
            name_node   = self.nameNode
            job_tracker = self.jobTracker
 
         
        REQEXC = (requests.exceptions.Timeout,         
                  requests.exceptions.ReadTimeout,     
                  requests.exceptions.ConnectTimeout,  
                  requests.exceptions.ConnectionError)
     
        #The request timed out.
        #The server did not send any data in the allotted amount of time.
        #The request timed out while trying to connect to the remote server.
        #A Connection error occurred.
         
        SUCCESS_CODES = (200,201,202)
         
        if kwargs is None or kwargs == {}:
            kwargs = {'timeout':self.timeout}
     
        if config_xml is None or type(config_xml) not in STRING_TYPES:
            raise TypeError("config_xml is a required argument to submit an Oozie requests")
        else:
            #Stand alone use
            content_header = {'Content-Type': 'application/xml;charset=UTF-8'}
            ingest_config_xml = config_xml
   
     
        for i in range(self.RETRIES):
            try:
                if start_job:
                    #Standard Job Submission
                    url = oozieHttpUrl + '/jobs?action=start'
                    self.logger.info("Submitting Oozie server request to start "
                                     "Hadoop file ingestion")
                    response = requests.post(url, data=ingest_config_xml, 
                                                      headers=content_header, **kwargs)
                    if response.status_code in SUCCESS_CODES:
                        result = response.json()
                        self.logger.info("Starting Oozie job '%s'"%result['id'])
                        if not get_object:
                            result = result['id']
                    else:
                        response.raise_for_status()
                     
                elif re_run:
                    url = oozieHttpUrl + '/job/%s?action=rerun'%oozie_id
                    self.logger.info("Submitting Oozie server request to rerun "
                                     "Hadoop file ingestion")
                    response = requests.post(url, data=ingest_config_xml, 
                                                      headers=content_header, **kwargs)
                    if response.status_code in SUCCESS_CODES:
                        result = response.json()
                        self.logger.info("Re-running Oozie job '%s'"%result['id'])
                        if not get_object:
                            result = result['id']
                    else:
                        response.raise_for_status()
                     
                elif get_status:
                    url = oozieHttpUrl + '/job/%s?show=info&timezone=GMT'%oozie_id
                    self.logger.info("Submitting Oozie server request to verify Hadoop "
                                     "file ingestion status")
                    response = requests.get(url, **kwargs)
                     
                    if response.status_code in SUCCESS_CODES:
                        result = response.json()
                        self.logger.debug("Status for Oozie job '%s' is '%s'"
                                          %(oozie_id,result['status']))
                        if not get_object:
                            result = result['status']
                    else:
                        response.raise_for_status()
                 
                elif get_logs:
                    url = oozieHttpUrl + '/job/%s?show=log'%oozie_id
                    self.logger.info("Submitting Oozie server request to retrieve "
                                     "logs for job id '%s"%oozie_id)
                    response = requests.get(url, **kwargs)
                     
                    if response.status_code in SUCCESS_CODES:
                        result = response.text.split('\n')
                        self.logger.debug("Logs for Oozie job '%s' retrieved successfully"
                                          %oozie_id)
                    else:
                        response.raise_for_status()
                         
                elif get_version:
                    url = oozieHttpUrl.rstrip('v1')+"versions"
                    self.logger.info("Submitting Oozie server request for Oozie versions")
                    response = requests.get(url, **kwargs)
                     
                    if response.status_code in SUCCESS_CODES:
                        result = response.text
                        self.logger.debug("Versions for Oozie server retrieved successfully")
                    else:
                        response.raise_for_status()
             
            except requests.exceptions.RequestException as e:
                traceback.print_exc(file=sys.stderr)
                self.logger.error("Received '%s' while submitting Oozie request"
                                  %e,exc_info=True)
                 
                if type(e) in REQEXC:
                    if i+1 != self.RETRIES:
                        self.logger.warning("Resubmitting Oozie request")
                        time.sleep(2)
                    else:
                        raise ResourceError("Oozie web services not available. %s"
                                            %e.message)
                else:
                    raise FileIngestionError("Oozie REST API error %s"%e.message)
 
            else:
                self.logger.debug("Successful Oozie server request. Submitted url '%s'"
                                  %url)
                break
                 
        return result