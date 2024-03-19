"""
Ubuntu fixes provided by Abhishek Kumar

Assumptions: 
- local vstart cluster is running
- appropriate caps were added to the vstart user (testid)

"""

import subprocess
import requests
import json
import hmac
import hashlib
import base64

def generate_signature(secret_key, http_request, contentType, dateTime, resource):
    headerToSign = f"{http_request}\n\n{contentType}\n{dateTime}\n{resource}"
    signature = hmac.new(secret_key.encode(), headerToSign.encode(), hashlib.sha1)
    return base64.b64encode(signature.digest()).decode()

def main():
    # Retrieve info from vstart user
    output = subprocess.check_output(['./bin/radosgw-admin', 'user', 'info', '--uid=testid']).decode('utf-8')
    output = json.loads(output)

    # Op parameters
    host = "localhost:8000"
    user = output['keys'][0]['user']
    access_key = output['keys'][0]['access_key']
    secret_key = output['keys'][0]['secret_key']
    http_request = 'GET'
    contentType = 'application/x-compressed-tar'
    resource = '/admin/user'
    http_query = 'info&uid=testid'
    dateTime = subprocess.check_output(['date', '-u', '+%Y%m%dT%H%M%SZ']).decode('utf-8').strip()
   
    signature = generate_signature(secret_key, http_request, contentType, dateTime, resource)

    # cURL call
    url = f'http://{host}{resource}?{http_query}'
    headers = {
        'Content-Type': contentType,
        'Date': dateTime,
        'Authorization': f'AWS {access_key}:{signature}',
        'Host': host
    }
   
    r = requests.get(url, headers=headers)

    # Output
    print("Read call, keys should be present:\n")
    print(r.json())

if __name__ == "__main__":
    main()
