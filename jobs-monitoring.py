import json
from operator import index, indexOf
import requests
import schedule
import time
import numpy as np
from functools import reduce

# Thresholds for the apps will be taken from json saved
with open('threshold.json', 'r') as f:
    threshold = json.load(f)

# REST API call to history server
response = requests.get("https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications")

def job():
    # Get specifics durations from response and converting into json readable
    response_json = response.json()
    #Getting duration and thresholds and Appids
    jobs_duration = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), response_json)), [])
    jobs_thresholds = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), threshold)), [])
    appIds = getAppId(response_json)

    # Evaluate current job duration > threshold maximum duration:
    print(jobs_duration, jobs_thresholds, 'Duration / Max threshold')
    # print(appIds, 'appIDs')
    result = np.less(jobs_thresholds, jobs_duration)
    print(result, 'jobs exceeded threshold')
    for i in result:
        if i==True:
            print('Maximum Threshold:', jobs_thresholds[i], ' exceeded. Latest job duration', jobs_duration[i], 'for appid: ', appIds[i])
    #notify_slack('Appids + jobs durations exceeded the threshold + thresholds')

# Send notification to slack channel
# SETUP - MUST BE HIDE IN Environment variable as showed by Kai
def notify_slack(message):
    payload = '{"text":"%s"}' % message
    response = requests.post('https://hooks.slack.com/services/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',
                            data=payload) # to hide later
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
# # Scheduling the job every 2 weeks
# schedule.every().day.at("12:10").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)