import json
import os
import time
from functools import reduce

import numpy as np
import requests
import schedule

# Thresholds for the apps will be taken from json saved
with open("threshold.json", "r") as f:
    threshold = json.load(f)

# REST API call to history server
response = requests.get(
    "https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications"
)

# Getting webhook for slack from env var
webhook = os.getenv("WEBHOOK")


def job():
    # Get specifics durations from response and converting into json readable
    response_json = response.json()
    # Getting duration and thresholds and Appids
    jobs_duration = sum(
        list(map(lambda val: reduce(getDuration, val["attempts"], []), response_json)),
        [],
    )
    jobs_thresholds = sum(
        list(map(lambda val: reduce(getDuration, val["attempts"], []), threshold)), []
    )
    serverAppIds = getAppId(response_json)
    threshAppIds = getAppId(threshold)

    # Evaluate current job duration > threshold maximum duration:
    # Jobs that exceeded threshold
    result = np.less(jobs_thresholds, jobs_duration)
    # index of the jobs exceeded threshold
    index = np.where(result == True)[0]

    if np.array_equal(serverAppIds,threshAppIds):
        for i in index:
            result = (
                jobs_duration[i],
                "duration exceeded the maximum threshold of",
                jobs_thresholds[i],
                "for appId:",
                serverAppIds[i],
            )
            message = " ".join(map(str, result))  # Perhaps not necessary
            print(message)
            # notify_slack(message)


# Send notification to slack channel
def notify_slack(message):
    payload = '{"text": "%s"}' % message
    r = requests.post(webhook, data=payload)
    print(r.text)


def getAppId(response_json):
    ids = []
    for item in response_json:
        my_dict = {}
        my_dict = item.get("id")
        ids.append(my_dict)
    return ids


def getDuration(array, attempt):
    array.append(attempt["duration"])
    return array


job()
# Scheduling the job every 2 weeks
# schedule.every(2).weeks.at("10:00").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)
