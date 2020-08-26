import io
import os
import sys
import time
import csv as cv
import pandas as pd
import numpy as np
import cx_Oracle as ocl
import splunklib.client as client
import splunklib.results as results

# Splunk Credentials
HOST = "xxx.xxx.xxx"
PORT = 0000
USERNAME = "xxxxxxx"
PASSWORD = "********"

# Create a Service instance and log in 

try:
    service = client.connect(
      host=HOST,
      port=PORT,
      username=USERNAME,
      password=PASSWORD)
except:
    print ("Unable to connect to splunk instance. Please update login credentials.\n")
    sys.exit(1)
           

# Create Search Query and Set Exec Mode

query = "search index=Connx90_Summary source=DNSExtract earliest=-90d latest=now\
|rename count(chostname) as frequency|rex field=shostname \"(?<f1>[^-]*)\"| rex field=f1 \"(?<hostname>[^_]*)\" \
|bucket span=7d _time| eval week_month=strftime(_time, \"%m--%d\") \
|stats avg(frequency) as avgFrequency by hostname chostname week_month | head 2000000"

mode = {"exec_mode": "blocking"}

# Run the job

job = service.jobs.create(query, **mode)

# Gather and print Job Properties

print "Search job properties"
print "Search job ID:        ", job["sid"]
print "The number of events: ", job["eventCount"]
print "The number of results:", job["resultCount"]
print "Search duration:      ", job["runDuration"], "seconds"
print "This job expires in:  ", job["ttl"], "seconds"

# Get the number of rows returned

#print "Search results:\n"
resultCount = job["resultCount"]

# Iterate through the results to paginate

offset = 0                       
count = 49000
resultsList = []

while (offset < int(resultCount)):
    kwargs_paginate = {"count": count,
                       "offset": offset}

    # Get the search results and append result to list
    blocksearch_results = job.results(**kwargs_paginate)

    for result in results.ResultsReader(io.BufferedReader(blocksearch_results)):
        
        resultsList.append(result)

    # Increase the offset to get the next set of results
    offset += count
    
print("........Done!\n")

# Export list of lists to pandas dataframe and pivot

pandasData = pd.DataFrame(resultsList)
splunkPivotData = pd.pivot_table(pandasData, values='avgFrequency', index=['hostname', 'chostname'], columns=['week_month'], aggfunc=sum, fill_value='')
#print(splunkPivotData)

# Check if file CSV file exists

fileName = "exportWeeklyAvg.csv"

if os.path.exists(fileName):
    os.remove(fileName)
    print("Existing File Removed. \n")
    time.sleep(5)
else:
    print("New file will be created. \n")
    

# Export results to CSV

splunkPivotData.to_csv(fileName)
print("Export to CSV completed succesfully!\n")
time.sleep(30)

# Connect to Oracle Database

tns = 'usn/pwd@xxx.xxx.xxx:0000/servicename'

try:
    con = ocl.connect(tns)
    print ("Connected to Oracle Instance. \n")
except:
    print ("Unable to connect to Oracle Instance. \n")
    sys.exit(1)

# Check if table is empty and truncate

checkQuery = "SELECT CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END FROM SPLUNKINFO_WAVG"
cur2 = con.cursor()
cur2.execute(checkQuery)
res = cur2.fetchall()
var = res[0][0]

if var == 1:
    print ("Rows exists! Contents will be truncated! \n")
    cur3 = con.cursor()
    cur3.execute("TRUNCATE TABLE SPLUNKINFO_WAVG")
    time.sleep(5)
    cur3.close()
else:
    print ("Table is empty! \n")

# Import Data from CSV to Oracle DB

cur = con.cursor()
cur.setinputsizes(150, 150, float, float, float, float, float, float, float, float, float, float, float, float, float)
sqlQuery = "INSERT INTO SPLUNKINFO_WAVG(SHOSTNAME, CHOSTNAME, WEEK1, WEEK2, WEEK3, WEEK4, WEEK5, WEEK6, WEEK7, WEEK8, WEEK9,\
            WEEK10, WEEK11, WEEK12, WEEK13) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"

numLines = 0
with open('exportWeeklyAvg.csv') as csvData:
    reader=cv.reader(csvData,delimiter=',')
    next (reader)
    for row in reader:
        #print(row)
        cur.execute(sqlQuery, row)
        numLines = numLines + 1


con.commit()
print ( str (numLines) + " rows inserted successfully. \n")
cur.close()
cur2.close()
con.close()





