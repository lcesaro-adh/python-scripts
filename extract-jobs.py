import json
import requests
import numpy as np
from functools import reduce

# Opening threshold json file and loading from json file
# with open('/Users/ludovicocesaro/Downloads/threshold.json', 'r') as f:
#   threshold = json.load(f) 

response = requests.get("https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications")
    # APP 0 
    # Get specific duration from response
response_json = response.json()

# Assuming max threshold
max_duration_threshold = 100000

# # # APP 0 values
# current_job_duration0 = threshold[0]['attempts'][0]['duration']
# print(current_job_duration0)

# # # APP 1 values
# current_job_duration1 = threshold[1]['attempts'][0]['duration']
# print(current_job_duration1)

# # Get duration of all apps in an array
def getDuration(array, attempt):
    array.append( attempt['duration'])
    return array

durations = sum(list(map(lambda val: reduce(getDuration, val['attempts'], []), response_json)), [])
#appNames = list(map(lambda val: reduce(getAppName, val['id'], []), response_json))

def getAppName(response_json):
  result = []
  for item in response_json:
      my_dict={}
      my_dict=item.get('id')
      result.append(my_dict)
  print(result)

print(durations) # array of app durations

getAppName(response_json)


# print(current_job_duration, 'CURRENT JOB DURATION')