
from concurrent import futures
from datetime import datetime, timedelta, date
import requests
import concurrent.futures
from requests.structures import CaseInsensitiveDict
import json
import threading
from threading import *
import time
from concurrent.futures import ThreadPoolExecutor
import collections
from google.cloud import storage
import math

url = "http://api.seonindonesia.net/sekolah"

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "token fc58379f2f373ea8a9a1535c642232ecdbf8a5ea"
headers["Content-Type"] = "application/json"

resp = requests.get(url, headers=headers)

resp = json.loads(resp.text)

urlLogSchedulerGlobal = "https://api.seonindonesia.net/log_scheduler/create"
headersPython2 = CaseInsensitiveDict()
headersPython2["Accept"] = "application/json"
headersPython2["Authorization"] = "token fc58379f2f373ea8a9a1535c642232ecdbf8a5ea"
headersPython2["Content-Type"] = "application/json"

datas = []

today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ambilData(key):
    datas.append({'urlSeonpoint': "https://" + key['sekolah_domain'] + "/api/main/seonpoint", \
                  'sekolah_domain': key['sekolah_domain'], \
                  'sekolah_bucket': key['sekolah_bucket']})


with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData, key) for key in resp]

for key in datas:

    if len(str(key['sekolah_bucket'])) > 0:
        #if key['sekolah_domain'] != "lms-sman22bdg.com":
        CLOUD_STORAGE_BUCKET = key['sekolah_bucket']

        storage_client = storage.Client()
        try:
            bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)
            blobs = bucket.list_blobs()

            total = 0
            for blob in blobs:
                total += blob.size
            size = str(total)

            url = "https://api.seonindonesia.net/growth_storage_usage/create"

            datas = {"size": size,"domain_sekolah":str(key['sekolah_domain']),"bucket":str(key['sekolah_bucket']),"date_record":str(today)}
            datas = str(datas).replace("'", '"')
            with ThreadPoolExecutor(max_workers=None) as exec:
                try:
                    resp = requests.post(url, headers=headers, data=datas)
                    #print(str(resp) + "berhasil")
                    log_scheduler_global = {"domain": key['sekolah_domain'],
                                            "kode_scheduler": "growth storage usage", "record_time": today,
                                            "status": "success", "error_message": ""}
                    log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                    resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)

                except Exception as e:
                    log_scheduler_global = {"domain": key['sekolah_domain'],
                                            "kode_scheduler": "growth storage usage", "record_time": today,
                                            "status": "failed", "error_message": str(e)}
                    log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                    resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
                    #print(str(resp) +"gagal1")
        except Exception as e:
            log_scheduler_global = {"domain": key['sekolah_domain'],
                                    "kode_scheduler": "growth storage usage", "record_time": today,
                                    "status": "failed", "error_message": str(e)}
            log_scheduler_global = str(log_scheduler_global).replace("'", '"')
            resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            #print(str(resp) + "gagal2")
        #else:
        #    log_scheduler_global = {"domain": key['sekolah_domain'],
        #                            "kode_scheduler": "update storage usage", "record_time": today,
        #                            "status": "failed", "error_message": "diupdate manual sama usep"}
        #    log_scheduler_global = str(log_scheduler_global).replace("'", '"')
        #    resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)


    else:
        log_scheduler_global = {"domain": key['sekolah_domain'],
                                "kode_scheduler": "growth storage usage", "record_time": today,
                                "status": "failed", "error_message": "bucket name is not define in the sekolah table"}
        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
