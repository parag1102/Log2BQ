from __future__ import with_statement

import re
import os
import cgi
import math
import time
import string
import urllib
import pickle
import pprint
import logging
import httplib2
import datetime
import wsgiref.handlers

try:
    import json                # Python 2.7.
except ImportError:
    import simplejson as json  # Python 2.5.


from google.appengine.ext import db
from bigqueryv2 import BigQueryClient
from apiclient.discovery import build
from apiclient.errors import HttpError
from google.appengine.api import files
from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.api import channel
from google.appengine.ext import deferred
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api import taskqueue
from google.appengine.api import app_identity
from google.appengine.ext.webapp import template
from google.appengine.api.logservice import logservice
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials

try:
    files.gs
except AttributeError:
    import gs
    files.gs = gs

###############################################################################

# Counts the application logs logged from our class and returns the same
def countapplogs():
    
    end_time = time.time()
    count=0
    
    for req_log in logservice.fetch(end_time=end_time, offset=None,
                                    minimum_log_level=logservice.LOG_LEVEL_INFO,
                                    include_app_logs=True):
        
        for app_log in req_log.app_logs:
            lmessage=app_log.message
            if (True == lmessage.startswith('%lg%')):
                count+=1
    
    return count

###############################################################################

# Calculates how many files to be created on Google Cloud Storage
def calculateLoopCount():
    
    totalcount=countapplogs()
    log_count=30
    loop_count=int(math.floor(totalcount/log_count))+1
    print ("<br />LoopCount: %d<br />Total Logs: %d<br />" %(loop_count,totalcount))

    return loop_count
 
###############################################################################

# Gets the logs logged from our class from app engine Log Store and appends them
# in a CSV string
def getapplogs(start_offset, how_many):
    
    end_time = time.time()
    i=0
    s=''
    end_offset=start_offset+how_many
    for req_log in logservice.fetch(end_time=end_time, offset=None,
                                    minimum_log_level=logservice.LOG_LEVEL_INFO,
                                    include_app_logs=True):
        
        for app_log in req_log.app_logs:
            if i in range(start_offset,end_offset):
                lmessage=app_log.message
                if (True == lmessage.startswith('%lg%')):
                    lmessage=re.sub('%lg%','',lmessage)
                    s=("""%s
%s""" %(lmessage,s))
            elif (i>end_offset):
                break
            i+=1
    
    if (''==s ):
        print 'No logging message specified<br />'
    
    return s

###############################################################################

# Import the table from Google Cloud Storage and print out the uploaded table.
def loadTable(service, projectId, datasetId, targetTableId, sourceCsv, schema):
    
    
    schema_json = json.loads('%s' % schema)
    
    try:
        jobCollection = service.jobs()
        jobData =  {
            "projectId": projectId,
            "configuration": {
                "load": {
                    "sourceUris": [sourceCsv],
                    "schema": schema_json,
                    "destinationTable": {
                        "projectId": projectId,
                        "datasetId": datasetId,
                        "tableId": targetTableId
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "encoding": "UTF-8"
        }
        }
        }
        
        
        insertResponse = jobCollection.insert(projectId=projectId,
                                              body=jobData).execute()
        
        # Ping for status until it is done, with a short pause between calls.
        import time
        while True:
            status = jobCollection.get(projectId=projectId,
                                       jobId=insertResponse['jobReference']['jobId']).execute()
            if 'DONE' == status['status']['state']:
                break
            print 'Waiting for the import to complete...'
            time.sleep(10)
        
        if 'errors' in status['status']:
            print 'Error loading table: ', pprint.pprint(status)
            return
        
        print '### Loaded the table ###<br />'
    
    except HttpError, err:
        print 'Error in loadTable: ', pprint.pprint(err.resp)

###############################################################################

# Writes the logs to Google Cloud Storage in a user specified CSV file
def writeToGS(csv_name,start_offset,how_many):
    #Sending to Google Storage
    
    read_path="/gs/" + csv_name
    
    # Create a file that writes to Cloud Storage and is readable by everyone in the project.
    write_path = files.gs.create(read_path, mime_type='text/plain', 
                                 acl='public-read')
    # Write to the file.
    with files.open(write_path, 'a') as fp:
        fp.write(getapplogs(start_offset,how_many))
    
    # Finalize the file so it is readable in Google Cloud Storage.
    files.finalize(write_path)
    
###############################################################################

# Sends the logs from Google Cloud Storage to the BigQuery table specified
# by the user
def sendToBQ(project_id, dataset_id, table_id, csv_name, schema):
    
    source_csv="gs://" + csv_name
    
    credentials = AppAssertionCredentials(
                                          scope='https://www.googleapis.com/auth/bigquery')
    
    http = credentials.authorize(httplib2.Http(memcache))
    service = build("bigquery", "v2", http=http)
    
    
    loadTable(service, project_id, dataset_id, table_id, source_csv, schema)
    
###############################################################################