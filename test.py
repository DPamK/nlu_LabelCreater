import requests
import json

url = "http://127.0.0.1:7777/register"

temp = {
  "labeler": "wang",
  "task": "test1",
  "num": 1,
  "cut_mode": 1,
  "id":13
}
temp = {
  'name':"yi",
  'password':"123456",
}

payload = json.dumps(temp)

headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(json.loads(response.text))
