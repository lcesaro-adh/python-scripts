import json
import requests
import schedule
import time
import numpy as np
from functools import reduce

# Thresholds for the apps will be taken from json saved
with open('/Users/ludovicocesaro/Downloads/threshold.json', 'r') as f:
    threshold = json.load(f)

# Assuming max threshold
# max_duration_threshold = 100000

def job():
    print("Get job infos from spark history server")
    # Getting the current status using the rest api
    response = requests.get("https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications")

    # Get specifics durations from response and converting into json readable
    response_json = response.json()
    jobs_duration = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), response_json)), [])
    jobs_thresholds = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), threshold)), [])
    print(jobs_duration, 'Current jobs duration')
    print(jobs_thresholds, 'Threshold jobs duration')

    appIds = getAppId(response_json)
    print(appIds, 'APP IDS')

    # Evaluate current job duration > threshold maximum duration:

        #notify_slack()

# Send notification to slack channel
# SETUP - MUST BE HIDE IN Environment variable as showed by Kai
def notify_slack(duration):
    payload = 'duration'
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
