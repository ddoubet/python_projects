#!/usr/bin/python
# Author: Dustin Doubet
# Description:
# Transfers files from S3 to HDFS verifies the checksum

import os
import sys
import hashlib
from io import BytesIO
from datetime import datetime, date, timedelta

import zipimport
sys.path.insert(0, '.')
import six
importer = zipimport.zipimporter('requests.zip')
requests = importer.load_module('requests')
import requests
importer = zipimport.zipimporter('hdfs.zip')
hdfs = importer.load_module('hdfs')
import hdfs
from hdfs import HdfsError
importer = zipimport.zipimporter('boto-2.32.1.zip')
boto = importer.load_module('boto')
from boto.s3.connection import S3Connection
from boto.utils import parse_ts

srcParentDir = ''
srcSubDir    = ''
part         = ''
fileSep      = '/'
dash         = '-'
uscore       = '_'
tmpExten     = ".tmp"

nameNode        = sys.argv[1] # 'http://webhdfs<>.com:50070/'
userName        = sys.argv[2] #'< user >'
ingestPath      = sys.argv[3].rstrip(fileSep)#'< hdfs path >'

fnPrefix        = sys.argv[9]
cleanPath       = bool(sys.argv[10])

conn   = S3Connection(ACCESSKEYID,SECRETACCESSKEY)
bucket = conn.get_bucket(BUCKETNAME, validate=False)
client = hdfs.client.InsecureClient(nameNode, user=userName)


if os.path.exists('outgoing'):
    shutil.rmtree('outgoing/')


processDate = datetime.strptime(year+'-'+month+'-'+day+' UTC', '%Y-%m-%d %Z')
dateScan = processDate - timedelta(days=7)



while dateScan <= processDate:
    year =str(dateScan.year)
    month = str(dateScan.month).zfill(2)
    day = str(dateScan.day).zfill(2)
    date = year+'-'+month+'-'+day
    path = 'outgoing/'+date
    os.makedirs(path+'/graph/')
    for file_key in bucket.list():
        print file_key.name
        if file_key.name.startswith(path+'/graph/part'):
            file_key.get_contents_to_filename(file_key.name)
            open(path+'/graph/checked','a').close()
            client.delete(ingestPath+'/outgoing/'+date, recursive = True)
            client.upload(ingestPath+'/outgoing/'+date, path)
    dateScan = dateScan + timedelta(days=1)


shutil.rmtree('outgoing')

scanDt    = datetime.strptime(dropDate + ' UTC', '%Y-%m-%d %Z')
processDt = datetime.strptime(processDate + ' UTC', '%Y-%m-%d %Z')

scanDate   = str(scanDt.year) + dash + str(scanDt.month).zfill(2) + dash + str(scanDt.day).zfill(2)
ingestDate = str(processDt.year) + dash + str(processDt.month).zfill(2) + dash + str(processDt.day).zfill(2)

mirrorPath = srcParentDir + fileSep + scanDate + fileSep + srcSubDir + fileSep
hdfsPath   = ingestPath + fileSep + srcParentDir + fileSep + ingestDate + fileSep + srcSubDir + fileSep

print "Listing S3 bucket " + BUCKETNAME + " for " + mirrorPath 
fetchCount      = 0
fetchSizeSum    = 0
createFileCount = 0
totalSize       = 0
totalCount      = 0
rmFileCount     = 0
maxBuffCreated  = 0 
createNew       = False
copyName        = ''
tempName        = ''
# Smallest HDFS block size 64 MiB
blockSizeList = [67108864, 134217728, 268435456, 536870912]
blockSize = min(blockSizeList)

for file_key in bucket.list(mirrorPath):
    totalSize += file_key.size
    totalCount += 1
    
print "Total files found in bucket: " + str(totalCount)
print "Total bytes in bucket: " + str(totalSize)
 
for bsize in blockSizeList:
    if totalSize > bsize:
        blockSize = bsize
        
print "Setting block size to " + str(blockSize)
    
try:
    for i, file_key in enumerate(bucket.list(mirrorPath)):
        print 'Found file entry ' + file_key.name
        print 'Checking if file entry starts with ' + mirrorPath + part
        if file_key.name.startswith(mirrorPath + part):
            fsize = file_key.size
            if (fsize + fetchSizeSum) > blockSize:
                createNew = True
            filename  = os.path.basename(file_key.name)
            fileExten = os.path.splitext(filename)[1]
            print 'Verifying and fetching file ' + file_key.name
            # Setting encoding to None returns just bytes
            for retry in xrange(3):
                contents = file_key.get_contents_as_string(encoding=None)
                m = hashlib.md5()
                m.update(contents)
                if file_key.md5 == m.hexdigest():
                    break
                else:
                    print "The md5 of file %s does not match"%filename
                    if retry == 2:
                        print "Exceeded the number of md5 file check retries"
                        raise ValueError("Could not verify md5 of file " + file_key.name)
            try:
                bio = BytesIO(contents)
                if bio.__sizeof__() > maxBuffCreated:
                    maxBuffCreated = bio.__sizeof__()
                if fetchCount == 0:
                    if cleanPath:
                        # TODO: Added a function that checks if a path exists
                        try:
                            client.list(hdfsPath)
                        except HdfsError as e:
                            if e.message.lower().find("does not exist") != -1:
                                print "Hdfs path %s does not exists"%hdfsPath
                        else:
                            for hdfsFile in client.list(hdfsPath):
                                if hdfsFile.startswith(fnPrefix):
                                    print 'Removing HDFS path ' + hdfsPath + hdfsFile
                                    client.delete(hdfsPath + hdfsFile)
                                    rmFileCount += 1
                                elif hdfsFile.startswith(uscore + fnPrefix):
                                    print 'Removing HDFS path ' + hdfsPath + hdfsFile
                                    client.delete(hdfsPath + hdfsFile)
                                    rmFileCount += 1
        
                    copyName = fnPrefix + uscore + filename.rstrip(fileExten) + uscore + str(fetchCount+1) + fileExten
                    tempName = uscore + copyName + tmpExten
                    print 'Creating file ' + hdfsPath + copyName
                    client.write(hdfsPath + tempName, 
                                 data=bio, append=False, blocksize=blockSize)
                    createFileCount += 1
                elif createNew:
                    print "Rename temp file %s to %s"%(tempName, copyName)
                    client.rename(hdfsPath + tempName, hdfsPath + copyName)
                    copyName = fnPrefix + uscore + filename.rstrip(fileExten) + uscore + str(fetchCount+1) + fileExten
                    tempName = uscore + copyName + tmpExten
                    print 'Creating file ' + hdfsPath + copyName
                    client.write(hdfsPath + tempName, 
                                 data=bio, append=False, blocksize=blockSize)
                    createNew = False
                    createFileCount += 1
                else:
                    print 'Appending to file ' + hdfsPath + tempName
                    client.write(hdfsPath + tempName, 
                                 data=bio, append=True, blocksize=None)
                fetchCount += 1
                fetchSizeSum += fsize
            finally:
                bio.close()
                
    print "Rename temp file %s to %s"%(tempName, copyName)
    client.rename(hdfsPath + tempName, hdfsPath + copyName)

finally:
    conn.close()
    print 'Total file entries matching pattern \'' + mirrorPath + part + '\' : ' + str(fetchCount)
    print 'Total file entries copied: ' + str(fetchCount)
    print 'Total bytes copied: ' + str(fetchSizeSum)
    print 'Largest file buffer created: ' + str(maxBuffCreated)
    print 'Tatal HDFS files cleaned: ' + str(rmFileCount)
    print 'Total HDFS files created: ' + str(createFileCount)
if fetchCount == 0:
    raise ValueError("No files found in bucket %s, matching pattern '%s%s'"%(BUCKETNAME,mirrorPath,part))

