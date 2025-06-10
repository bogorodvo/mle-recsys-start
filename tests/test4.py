import requests

recommendations_url = "http://127.0.0.1:8000"

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
params = {"user_id": 1291248, 'k': 3}

resp = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
online_recs = resp.json()
    
print(online_recs) 

events_store_url = "http://127.0.0.1:8020"

params = {"user_id": 1291248, "item_id": 17245}

# resp = requests.post(events_store_url + "/put", headers=headers, params=params) 