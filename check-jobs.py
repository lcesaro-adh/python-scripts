import json
import requests
import schedule
import time

# Thresholds for the apps will be taken from json saved
    # with open('/Users/ludovicocesaro/Downloads/threshold.json', 'r') as f:
    #   threshold = json.load(f) 

# Assuming max threshold
max_duration_threshold = 100000

def job():
    print("Get job infos from spark history server")
    # Getting the current status using the rest api
    response = requests.get("https://adh-dsw-spark-history.dev.adh.syncier.cloud/api/v1/applications")
    # APP 0 
    # Get specific duration from response
    response_json = response.json()
    response_attempts = response_json[0]['attempts']
    current_job_duration = response_attempts[0]['duration']

    print(current_job_duration, 'CURRENT JOB DURATION')

    # Evaluate current job duration > threshold maximum duration:
    if current_job_duration > max_duration_threshold:
        print('threshold surpassed')
        notify_slack(current_job_duration)

# Send notification to slack channel
# SETUP - MUST BE HIDE IN Environment variable as showed by Kai
def notify_slack(duration):
    payload = 'duration'
    response = requests.post('https://hooks.slack.com/services/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',
                            data=payload) # to hide later
    print(response.text)

# Scheduling the job every 2 weeks
schedule.every().day.at("12:10").do(job)
#schedule.every().seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
