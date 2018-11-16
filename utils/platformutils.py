

import os
import sys
import ntpath
import warnings
import logging
import platform
import collections


try:
    import psutil
    import numpy as np
except ImportError as e:
    deprec = True
    warnings.warn("System package dependency not installed"
                   "platformutils.py will use depreciated methods.")
else:
    deprec = False
    
    
class SysResMonitorError(Exception):
    pass

class DeprecMethodError(SysResMonitorError):
    
    def __init__(self, message, errors=None):

        #if message.startswith('default'):
        if type(message) == unicode:
            message = message.encode('ascii','replace')
            # Call the base class constructor with the parameters it needs
        super(DeprecMethodError, self).__init__(message)

            # Now for your custom code...
        self.errors = errors
        
class ReferenceTypeError(SysResMonitorError):
    
    def __init__(self, message, errors=None):

        #if message.startswith('default'):
        if type(message) == unicode:
            message = message.encode('ascii','replace')
            # Call the base class constructor with the parameters it needs
        super(ReferenceTypeError, self).__init__(message)

            # Now for your custom code...
        self.errors = errors
        
        
class SysResMonitorClass:
    """doc string"""
    
    @classmethod
    def get_proc_pid(self,pid=None):
        
        if pid == None:
            proc_status = '/proc/%d/status'%os.getpid()
        else:
            proc_status = '/proc/%d/status'%pid
            
        return proc_status
    
    @classmethod
    def get_proc_scale(self):
        """doc string"""
        
        scale = {'kB': np.int32(1024), 'mB': np.int32(1048576),
                  'KB': np.int32(1024), 'MB': np.int32(1048576),
                  'gB':np.int32(1073741824),'bytes':np.int32(1),
                  'GB':np.int32(1073741824)}
        
        return scale

    @classmethod
    def memory_percent(self,scaleKey='bytes',since=0.0,pid=None):
        """doc string"""
        if deprec:
            raise DeprecMethodError("'memory_percent' is not an available method. "
                                    "Required Python packages 'psutil' and 'numpy' are needed.")
        elif pid != None:
            p = psutil.Process(pid)
        else:
            p = psutil.Process()
           
        return np.float32(p.memory_percent() - since)
    
    @classmethod
    def proc_memory(self,VmKey,pid=None):
        """Will read linux type pseudo file and get the current
           pid memory depending ont he memory type key found and 
           read in the file."""
        
        warnings.warn("WARNING: Using a depreciated method 'proc_memory'. "
                      "Not all system resource methods will be available.",UserWarning)
        
        proc_status = self.get_proc_pid(pid)
        scale = self.get_proc_scale()
         #get pseudo file  /proc/<pid>/status
        try:
            t = open(proc_status)
            v = t.read()
        except:
            return 0.0 #non-Linux?
        finally:
            t.close()
        #get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
        i = v.index(VmKey)
        v = v[i:].split(None, 3)  # whitespace
        if len(v) < 3:
            print "Not a valid format for reading pseudo file  /proc/<pid>/status."
            return 0.0 #invalid format?
        #convert Vm value to bytes
        return float(v[1]) * scale[v[2]]

    @classmethod
    def proc_memory(self,scaleKey='bytes',since=0.0,pid=None):
        """Return virtual memory for current process. Memory size is
           returned depending on the scale dictionary and scaleKey entered."""
        if deprec:
            return float(self.proc_memory('VmSize:')/self.get_proc_scale()[scaleKey]) - since
        
        elif pid != None:
            p = psutil.Process(pid)
        else:
            p = psutil.Process()
           
        return np.float32(p.memory_info_ex().vms/self.get_proc_scale()[scaleKey]) - since
    
    @classmethod
    def proc_resident(self,scaleKey='bytes',since=0.0,pid=None):
        """Return resident memory usage in bytes. This will give 
           shared and unshared memory usage. Not as useful if multiple
           processes share same memory; memory will be counted twice 
           between them."""
        if deprec:
            return float(self.proc_memory('VmRSS:',pid)/self.get_proc_scale()[scaleKey]) - since
        
        elif pid != None:
            p = psutil.Process(pid)
        else:
            p = psutil.Process()
                
        return np.float32(p.memory_info_ex().rss/self.get_proc_scale()[scaleKey]) - since
    
    @classmethod    
    def proc_all(self,pid=None,scaleKey='bytes',since=None,returnDict=False):
        """doc string"""
        
        if deprec:
            raise DeprecMethodError("'proc_all' is not an available method. "
                                    "Required Python packages 'psutil' and 'numpy' are needed.")
        elif pid != None:
            p = psutil.Process(pid)
        else:
            p = psutil.Process()
            
        scale = self.get_proc_scale()
        
        if returnDict:
            procAllDict = p.memory_info_ex()._asdict()
            if since != None:
                if type(since) == dict or type(since) == collections.OrderedDict:
                    for key in procAllDict.keys():
                        procAllDict[key] = np.float32(procAllDict[key]/scale[scaleKey]) - since[key]
                        
                elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                    sinceDict = since._asdict()
                    for key in procAllDict.keys():
                        procAllDict[key] = np.float32(procAllDict[key]/scale[scaleKey]) - sinceDict[key]
                else:
                    raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                             "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
            else:
                for key in procAllDict.keys():
                    procAllDict[key] = np.float32(procAllDict[key]/scale[scaleKey])
                        
            return procAllDict
        
        elif since == None:
            if scaleKey == 'bytes':
                return p.memory_info_ex()
            else:
                procAllDict = p.memory_info_ex()._asdict()
                for key in procAllDict.keys():
                    procAllDict[key] = np.float32(procAllDict[key]/scale[scaleKey])
                memoryInfo = collections.namedtuple('proc_memory_info',field_names=procAllDict.keys())
                return memoryInfo(**procAllDict)
            
        else:
            procAllDict = p.memory_info_ex()._asdict()
            if since != None:
                if type(since) == dict or type(since) == collections.OrderedDict:
                    for key in procAllDict.keys():
                        procAllDict[key] = np.float32(procAllDict[key]/scale[scaleKey]) - since[key]
                        
                elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                    sinceDict = since._asdict()
                    for key in procAllDict.keys():
                        procAllDict[key] =np.float32(procAllDict[key]/scale[scaleKey]) - sinceDict[key]
                else:
                    raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                             "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
     
            memoryInfo = collections.namedtuple('proc_memory_info',field_names=procAllDict.keys())
            return memoryInfo(**procAllDict)
        
    @classmethod
    def virtual_memory(self,scaleKey='bytes',since=None,returnDict=False):
        """doc string"""
        
        scale = self.get_proc_scale()
        
        if deprec:
            raise DeprecMethodError("'virtual_memory' is not an available method. "
                                    "Required Python packages 'psutil' and 'numpy' are needed.")
            
        elif returnDict:
            vmDict = psutil.virtual_memory()._asdict()
            if since != None:
                if type(since) == dict or type(since) == collections.OrderedDict:
                    for key in vmDict.keys():
                        if key != 'percent':
                            vmDict[key] = np.float32((np.float32(vmDict[key])/scale[scaleKey]) - since[key])
                        else:
                            vmDict[key] = np.float32(vmDict[key] - since[key])
                        
                elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                    sinceDict = since._asdict()
                    for key in vmDict.keys():
                        if key != 'percent':
                            vmDict[key] = np.float32((np.float32(vmDict[key])/scale[scaleKey]) - sinceDict[key])
                        else:
                            vmDict[key] = np.float32(vmDict[key] - sinceDict[key])
                else:
                    raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                             "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
            else:
                for key in vmDict.keys():
                    vmDict[key] = np.float32(vmDict[key])/scale[scaleKey]
                    
            return vmDict
        
        elif since == None:
            if scaleKey == 'bytes':
                return psutil.virtual_memory()
            else:
                vmDict = psutil.virtual_memory()._asdict()
                for key in vmDict.keys():
                    if key != 'percent':
                        vmDict[key] = np.float32(vmDict[key])/scale[scaleKey]
                        
                virtualMemory = collections.namedtuple('virtual_memory',field_names=vmDict.keys())
                return virtualMemory(**vmDict)
            
        else:
            vmDict = psutil.virtual_memory()._asdict()
            if type(since) == dict or type(since) == collections.OrderedDict:
                for key in vmDict.keys():
                    if key != 'percent':
                        vmDict[key] = np.float32((np.float32(vmDict[key])/scale[scaleKey]) - since[key])
                    else:
                        vmDict[key] = np.float32(vmDict[key] - since[key])

            elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                sinceDict = since._asdict()
                for key in vmDict.keys():
                    if key != 'percent':
                        vmDict[key] = np.float32((np.float32(vmDict[key])/scale[scaleKey]) - sinceDict[key])
                    else:
                        vmDict[key] = np.float32(vmDict[key] - sinceDict[key])
            else:
                raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                         "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
                
            virtualMemory = collections.namedtuple('virtual_memory',field_names=vmDict.keys())
            return virtualMemory(**vmDict)
        
    @classmethod
    def swap_memory(self,scaleKey='bytes',since=None,returnDict=False):
        """doct string"""
        
        scale = self.get_proc_scale()
        
        if deprec:
            raise DeprecMethodError("'swap_memory' is not an available method. "
                                    "Required Python packages 'psutil' and 'numpy' are needed.")
            
        elif returnDict:
            smDict = psutil.swap_memory()._asdict()
            if since != None:
                if type(since) == dict or type(since) == collections.OrderedDict:
                    for key in smDict.keys():
                        if key != 'percent':
                            smDict[key] = np.float32((np.float32(smDict[key])/scale[scaleKey]) - since[key])
                        else:
                            smDict[key] = np.float32(smDict[key] - since[key])
                        
                elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                    sinceDict = since._asdict()
                    for key in smDict.keys():
                        if key != 'percent':
                            smDict[key] = np.float32((np.float32(smDict[key])/scale[scaleKey]) - sinceDict[key])
                        else:
                            smDict[key] = np.float32(smDict[key] - sinceDict[key])
                else:
                    raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                             "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
            else:
                for key in smDict.keys():
                    if key != 'percent':
                        smDict[key] = np.float32(smDict[key])/scale[scaleKey]
                    
            return smDict
        
        elif since == None:
            if scaleKey == 'bytes':
                return psutil.swap_memory()
            else:
                smDict = psutil.swap_memory()._asdict()
                for key in smDict.keys():
                    if key != 'percent':
                        smDict[key] = np.float32(smDict[key])/scale[scaleKey]
                        
                swapMemory = collections.namedtuple('swap_memory',field_names=smDict.keys())
                return swapMemory(**smDict)
            
        else:
            smDict = psutil.swap_memory()._asdict()
            if type(since) == dict or type(since) == collections.OrderedDict:
                for key in smDict.keys():
                    if key != 'percent':
                        smDict[key] = np.float32((np.float32(smDict[key])/scale[scaleKey]) - since[key])
                    else:
                        smDict[key] = np.float32(smDict[key] - since[key])

            elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                sinceDict = since._asdict()
                for key in smDict.keys():
                    if key != 'percent':
                        smDict[key] = np.float32((np.float32(smDict[key])/scale[scaleKey]) - sinceDict[key])
                    else:
                        smDict[key] = np.float32(smDict[key] - sinceDict[key])
            else:
                raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                         "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
                
            swapMemory = collections.namedtuple('swap_memory',field_names=smDict.keys())
            return swapMemory(**smDict)
        
    @classmethod
    def proc_io_stats(self,pid=None,scaleKey='bytes',since=None,returnDict=False):
        """doc string"""
        
        if deprec:
            raise DeprecMethodError("'proc_io_stats' is not an available method. "
                                    "Required Python packages 'psutil' and 'numpy' are needed.")
        elif pid != None:
            p = psutil.Process(pid)
        else:
            p = psutil.Process()
            
        scale = self.get_proc_scale()
            
        if returnDict:
            ioDict = p.io_counters()._asdict()
            if since != None:
                if type(since) == dict or type(since) == collections.OrderedDict:
                    for key in ioDict.keys():
                        ioDict[key] = np.float32((np.float32(ioDict[key])/scale[scaleKey]) - since[key])
                        
                elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                    sinceDict = since._asdict()
                    for key in ioDict.keys():
                        ioDict[key] = np.float32((np.float32(ioDict[key])/scale[scaleKey]) - sinceDict[key])
                        
                else:
                    raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                             "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
            else:
                for key in ioDict.keys():
                    ioDict[key] = np.float32(ioDict[key])/scale[scaleKey]
                    
            return ioDict
        
        elif since == None:
            if scaleKey == 'bytes':
                return p.io_counters()
            else:
                ioDict = p.io_counters()._asdict()
                for key in ioDict.keys():
                    ioDict[key] = np.float32(ioDict[key])/scale[scaleKey]
                        
                procIOStats = collections.namedtuple('pio',field_names=ioDict.keys())
                return procIOStats(**ioDict)
            
        else:
            ioDict = p.io_counters()._asdict()
            if type(since) == dict or type(since) == collections.OrderedDict:
                for key in ioDict.keys():
                    ioDict[key] = np.float32((np.float32(ioDict[key])/scale[scaleKey]) - since[key])

            elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                sinceDict = since._asdict()
                for key in ioDict.keys():
                    ioDict[key] = np.float32((np.float32(ioDict[key])/scale[scaleKey]) - sinceDict[key])
            
            else:
                raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                         "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
                
            procIOStats = collections.namedtuple('pio',field_names=ioDict.keys())
            return procIOStats(**ioDict)
        
    @classmethod    
    def disk_usage(self,path,scaleKey='bytes',since=None,returnDict=False):
        """doc string"""
        
        scale = self.get_proc_scale()
        
        if deprec:
            raise DeprecMethodError("'disk_usage' is not an available method. "
                                    "Required Python packages 'psutil' and 'numpy' are needed.")
            
        elif returnDict:
            duDict = psutil.disk_usage(path)._asdict()
            if since != None:
                if type(since) == dict or type(since) == collections.OrderedDict:
                    for key in duDict.keys():
                        if key != 'percent':
                            duDict[key] = np.float32((np.float32(duDict[key])/scale[scaleKey]) - since[key])
                        else:
                            duDict[key] = np.float32(duDict[key] - since[key])
                        
                elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                    sinceDict = since._asdict()
                    for key in duDict.keys():
                        if key != 'percent':
                            duDict[key] = np.float32((np.float32(duDict[key])/scale[scaleKey]) - sinceDict[key])
                        else:
                            duDict[key] = np.float32(duDict[key] - sinceDict[key])
                else:
                    raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                             "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
            else:
                for key in duDict.keys():
                    if key != 'percent':
                        duDict[key] = np.float32(duDict[key])/scale[scaleKey]
                    
            return duDict
        
        elif since == None:
            if scaleKey == 'bytes':
                return psutil.disk_usage(path)
            else:
                duDict = psutil.disk_usage(path)._asdict()
                for key in duDict.keys():
                    if key != 'percent':
                        duDict[key] = np.float32(duDict[key])/scale[scaleKey]
                        
                diskUsage = collections.namedtuple('disk_usage',field_names=duDict.keys())
                return diskUsage(**duDict)
            
        else:
            duDict = psutil.disk_usage(path)._asdict()
            if type(since) == dict or type(since) == collections.OrderedDict:
                for key in duDict.keys():
                    if key != 'percent':
                        duDict[key] = np.float32((np.float32(duDict[key])/scale[scaleKey]) - since[key])
                    else:
                        duDict[key] = np.float32(duDict[key] - since[key])

            elif type(since) == psutil._pslinux.pextmem or isinstance(since,tuple):
                sinceDict = since._asdict()
                for key in duDict.keys():
                    if key != 'percent':
                        duDict[key] = np.float32((np.float32(duDict[key])/scale[scaleKey]) - sinceDict[key])
                    else:
                        duDict[key] = np.float32(duDict[key] - sinceDict[key])
            else:
                raise ReferenceTypeError("Input argument 'since' references to keys/field_names "
                                         "in dict, collections.OrderedDict, or psutil._pslinux.pextmem types")
                
            diskUsage = collections.namedtuple('disk_usage',field_names=duDict.keys())
            return diskUsage(**duDict)



class PlatformUtilsClass(SysResMonitorClass):
 
    @staticmethod
    def libraries():
        """doc string"""
        return ('os','sys','platform','ntpath','collections','psutil','numpy')
    
    @classmethod
    def get_platform(self):
        """doc string"""
        
        if 'windows' in platform.system() or 'Windows' in platform.system() or 'WINDOWS' in platform.system():
            return 'Windows'

        elif 'linux' in platform.system() or 'Linux' in platform.system() or 'LINUX' in platform.system():
            return 'Linux'

        else:
            return platform.system()
        
    @classmethod
    def file_exten(self,path,rtnFilename=False):
        """doc string"""
        
        splitPath = os.path.splitext(path)
        
        filename = splitPath[0]
        #strip extension just in case it has '\n' and lower to find all upper and lower file extensions
        extension = splitPath[1][1:].strip().lower()
        if rtnFilename:
            return filename, extension
        else:
            return extension

    @classmethod
    def path_leaf(self,path,fullDir=True,fileExten=False,**kwds):
        """doc string""" 
            
        platform = self.get_platform()
        
        head, tail = ntpath.split(path.strip('\\').strip('/'))

        if fileExten:
            pass
        else:
            tail, extension = self.file_exten(tail,rtnFilename=True)

        if platform == 'Windows':
            if fullDir:
                head = head+'\\'
            else:
                head = head
        elif platform == 'Linux':
            if fullDir:
                head = '/'+head+'/'
            else:
                head = '/'+head

        if fullDir:
            return head, tail
        else:
            return ntpath.basename(head), tail
        
    @staticmethod    
    def platform_info():
        """doc string"""
        
        def linux_distribution():
          try:
            return platform.linux_distribution()
          except:
            return "N/A"
        
        #sys.getfilesystemencoding()

        return """Python version: %s, dist: %s
        linux_distribution: %s
        system: %s
        machine: %s
        platform: %s
        uname: %s
        version: %s
        mac_ver: %s
        """ %(
        sys.version.split('\n'),
        str(platform.dist()),
        linux_distribution(),
        platform.system(),
        platform.machine(),
        platform.platform(),
        platform.uname(),
        platform.version(),
        platform.mac_ver())  
        