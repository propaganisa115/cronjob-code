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
urlLog = "http://api.seonindonesia.net/log_scheduler/create"

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "token fc58379f2f373ea8a9a1535c642232ecdbf8a5ea"
headers["Content-Type"] = "application/json"

resp = requests.get(url, headers=headers)

resp = json.loads(resp.text)
# print(resp)
datas = []


def ambilData(key):
    datas.append({'sekolah_domain': key['sekolah_domain'], 'id': key['id']})


with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData, key) for key in resp]

headersClient = CaseInsensitiveDict()
headersClient[
    "Authorization"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiU0VPTkFETTAwMSIsImZ1bGxuYW1lIjoiU2VvbiBBZG1pbiIsImVtYWlsIjoic2VvbmFkbWluQHNlb24uY29tIiwidHlwZSI6InN1cGVyX2FkbWluIiwiQVBJX1RJTUUiOjE2MjcwMjM3Mzl9.4HiC4XnwiQ6CpfpCg_IVqVQeLr0pwqT-pIEjRPb6dvQ"
headersClient["Content-Type"] = "application/json"
headersClient["Content-Length"] = "0"

urlLogSchedulerGlobal = "https://api.seonindonesia.net/log_scheduler/create"
headersPython2 = CaseInsensitiveDict()
headersPython2["Accept"] = "application/json"
headersPython2["Authorization"] = "token fc58379f2f373ea8a9a1535c642232ecdbf8a5ea"
headersPython2["Content-Type"] = "application/json"

yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
filter = {"start_date": yesterday, "end_date": yesterday}
filter = str(filter).replace("'", '"')

yesterdayOnlydate = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
filterOnlydate = {"date": yesterdayOnlydate}
filterOnlydate = str(filterOnlydate).replace("'", '"')

today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for key in datas:
    urlbnc = "https://" + str(key['sekolah_domain']) + "/api/main/backup"
    if len(str(key['sekolah_domain'])) < 0:
        log_scheduler_global = {"domain": "empty", "kode_scheduler": "scraping request of backup & cleansing data", "record_time": today,
                                "status": "failed", "error_message": "sekolah_domain is empty with id school " + str(
                key['id']) + ", you can check in the table sekolah"}
        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2)
    else:
        try:
            respClient = requests.get(urlbnc, headers=headersClient)

            if (respClient.status_code == 200):

                resp_json = json.loads(respClient.text)
                data_topup_storage = []

                domain = resp_json['sekolah']
                print(domain)


                def ambilData(key):
                    data_topup_storage.append(
                        {'id': str(key['id']), 'type': str(key['type']), 'proses_status': str(key['proses_status']),
                         'title': str(key['title']), 'created_at': str(key['created_at']),
                         'updated_at': str(key['updated_at']),
                         'estimated_time': str(key['estimated_time']), 'link_backup': str(key['link_backup']),
                         'sekolah_domain': str(domain)})



                with ThreadPoolExecutor(max_workers=None) as exec:
                    fut = [exec.submit(ambilData, key) for key in resp_json['data']]

                print(data_topup_storage)

                urlbncserver = "https://api.seonindonesia.net/backup_cleansing/create"
                headersPython = CaseInsensitiveDict()
                headersPython["Accept"] = "application/json"
                headersPython["Authorization"] = "token fc58379f2f373ea8a9a1535c642232ecdbf8a5ea"
                headersPython["Content-Type"] = "application/json"

                def tambahData(key):
                    TambahData = {"domain_sekolah": key['sekolah_domain'], "id_backup": key['id'], "type": key['type'],
                                  "proses_status": key['proses_status'], "title": key['title'], "created_at_client": key['created_at'],
                                  "updated_at": key['updated_at'], "estimated_time": key['estimated_time'], "link_backup": key['link_backup']
                                  ,"created_at_server": str(today)}
                    tambahdatas = str(TambahData).replace("'", '"')
                    #print(tambahdatas)
                    resp = requests.post(urlbncserver, headers=headersPython, data=tambahdatas)
                    update = {"id": [key['id']]}
                    update = str(update).replace("'", '"')
                    print(update)
                    try:
                        respClient = requests.put(urlbnc, headers=headersClient, data=update)
                        print(respClient)
                    except Exception as e:
                        error = str(e).replace("'", "")
                        print(error)
                    # print(resp)


                with ThreadPoolExecutor(max_workers=None) as exec:
                    try:
                        futures = [exec.submit(tambahData, key) for key in data_topup_storage]
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping request of backup & cleansing data", "record_time": today,
                                                "status": "success", "error_message": ""}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
                    except Exception as e:
                        error = str(e).replace("'", "")
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping request of backup & cleansing data", "record_time": today,
                                                "status": "failed", "error_message": error[:200]}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            else:
                log_scheduler_global = {"domain": str(key['sekolah_domain']),
                                        "kode_scheduler": "scraping request of backup & cleansing data", "record_time": today,
                                        "status": "failed",
                                        "error_message": "response return http error with status code " + str(
                                            respClient.status_code)}
                log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
        except Exception as e:
            error = str(e).replace("'", "")
            log_scheduler_global = {"domain": str(key['sekolah_domain']), "kode_scheduler": "scraping request of backup & cleansing data",
                                    "record_time": today, "status": "failed", "error_message": error[:200]}
            log_scheduler_global = str(log_scheduler_global).replace("'", '"')
            resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            # log_scheduler = {"domain":key['sekolah_domain'],"record_time": today ,"status":"failed","error_message":error[:100] }
            # log_scheduler =str(log_scheduler).replace("'", '"')
            # respLog = requests.post(log_scheduler, headers=headers, data=log_scheduler)
