import json
import requests
import schedule
import time
import numpy as np
from functools import reduce
import os

# Thresholds for the apps will be taken from json saved
with open('threshold.json', 'r') as f:
    threshold = json.load(f)

# REST API call to history server
response = requests.get("https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications")

#Getting webhook link for slack from env var
webhook = os.getenv('WEBHOOK')

def job():
    # Get specifics durations from response and converting into json readable
    response_json = response.json()
    #Getting duration and thresholds and Appids
    jobs_duration = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), response_json)), [])
    jobs_thresholds = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), threshold)), [])
    appIds = getAppId(response_json)

    # Evaluate current job duration > threshold maximum duration:
    # Jobs that exceeded threshold
    result = np.less(jobs_thresholds, jobs_duration)
    # index of the jobs exceeded threshold
    index = np.where(result == True)[0]

    for i in index: # improve message for slack
        print(jobs_duration[i], 'duration exceeded the maximum threshold of',jobs_thresholds[i], 'for appId:', appIds[i])

        #notify_slack(message)

# Send notification to slack channel
def notify_slack(message):
    payload = '{"text":"%s"}' % message
    response = requests.post(webhook,
                            data=payload)
    print(response.text)

def getAppId(response_json):
  ids = []
  for item in response_json:
      my_dict={}
      my_dict=item.get('id')
      ids.append(my_dict)
  return ids

def getDuration(array, attempt):
    array.append( attempt['duration'])
    return array

job()
# Scheduling the job every 2 weeks
# schedule.every().day.at("12:10").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)