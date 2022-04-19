import requests

webhook = 'Webhook-link'
message = 'Content of the message'

def notify_slack(message):
    payload = '{"text":"%s"}' % message
    response = requests.post(f'webhook',
                            data=payload) # to hide later
    print(response.text)


notify_slack(message)
