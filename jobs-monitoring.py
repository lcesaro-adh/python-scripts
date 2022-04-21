import json
import os
import time
from functools import reduce

import numpy as np
import requests
import schedule

# !! env var must be set with webhook link !!

# Getting thresholds from json
with open("thresholds.json", "r") as f:
    threshold = json.load(f)

# REST API call to history server to get info on apps
response = requests.get(
    "https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications"
)

# Getting webhook for slack from env var
webhook = os.getenv("WEBHOOK")


def job():
    # Converting json readable
    response_json = response.json()
    # Getting duration and max thresholds for apps
    jobs_duration = sum(
        list(map(lambda val: reduce(getDuration, val["attempts"], []), response_json)),
        [],
    )
    jobs_thresholds = sum(
        list(map(lambda val: reduce(getDuration, val["attempts"], []), threshold)), []
    )

    try:
        # Getting appids from HS and threshold
        serverAppIds = getAppId(response_json)
        threshAppIds = getAppId(threshold)
        # Check whether appids on threshold and server are the same
        if np.array_equal(serverAppIds, threshAppIds):
            # Evaluate current job duration > threshold maximum duration:
            # Jobs that exceeded threshold
            result = np.less(jobs_thresholds, jobs_duration)
            # index of the jobs exceeded threshold
            index = np.where(result == True)[0]
            for i in index:
                result = (
                    jobs_duration[i],
                    "duration exceeded the maximum threshold of",
                    jobs_thresholds[i],
                    "for appId:",
                    serverAppIds[i],
                )
                # Formatting to sring
                message = " ".join(map(str, result))
                print(message)
                # notify_slack(message)
        else:
            message = "Error: App ids of the thresholds are not matching with the history server ones"
            print(message)
            notify_slack(message)
    except Exception as e:
        # Notify any other error
        print(e)
        notify_slack(e)


# Send notification to slack channel
def notify_slack(message):
    payload = {"text": message}
    r = requests.post(webhook, data=json.dumps(payload))
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


# Scheduling the job every 2 weeks
schedule.every(14).days.at("10:30").do(job)

# testing purposes
# schedule.every(10).seconds.do(job)
# job()


while True:
    schedule.run_pending()
    time.sleep(1)
