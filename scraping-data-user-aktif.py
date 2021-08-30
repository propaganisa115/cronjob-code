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
headers["Authorization"] = "Basic YW5pc2E6YW5pc2E="
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
headersPython2["Authorization"] = "Basic YW5pc2E6YW5pc2E="
headersPython2["Content-Type"] = "application/json"

yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
filter = {"start_date": yesterday, "end_date": yesterday}
filter = str(filter).replace("'", '"')

yesterdayOnlydate = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
filterOnlydate = {"date": yesterdayOnlydate}
filterOnlydate = str(filterOnlydate).replace("'", '"')

today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for key in datas:
    urldata_user = "https://" + str(key['sekolah_domain']) + "/api/main/useronline/day"
    if len(str(key['sekolah_domain'])) < 0:
        log_scheduler_global = {"domain": "empty", "kode_scheduler": "scraping data user", "record_time": today,
                                "status": "failed", "error_message": "sekolah_domain is empty with id school " + str(
                key['id']) + ", you can check in the table sekolah"}
        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
    else:
        try:
            respClient = requests.get(urldata_user, headers=headersClient)

            if (respClient.status_code == 200):

                resp_json = json.loads(respClient.text)
                for c in resp_json['data']:
                    siswa=c['siswa']
                    guru = c['guru']
                    orang_tua = c['orang_tua']
                    total = c['total']
                domain=resp_json['sekolah']
                data_user=[]

                data_user.append({'siswa': siswa,'guru': guru,'orangtua': orang_tua,'total': total, 'sekolah_domain': str(domain),'record_time': today})



                urlSeon = "https://api.seonindonesia.net/data_user_aktif/create"
                headersPython = CaseInsensitiveDict()
                headersPython["Accept"] = "application/json"
                headersPython["Authorization"] = "Basic YW5pc2E6YW5pc2E="
                headersPython["Content-Type"] = "application/json"


                def tambahData(key):
                    TambahData = {"siswa": key['siswa'],"guru": key['guru'],"orangtua": key['orangtua'],"total": key['total'], "sekolah_domain": key['sekolah_domain'], "record_time": key['record_time']}
                    tambahdatas = str(TambahData).replace("'", '"')
                    resp = requests.post(urlSeon, headers=headersPython, data=tambahdatas)


                with ThreadPoolExecutor(max_workers=None) as exec:
                    try:
                        futures = [exec.submit(tambahData, key) for key in data_user]
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping data user", "record_time": today,
                                                "status": "success", "error_message": ""}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
                    except Exception as e:
                        error = str(e).replace("'", "")
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping data user", "record_time": today,
                                                "status": "failed", "error_message": error[:200]}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            else:
                log_scheduler_global = {"domain": str(key['sekolah_domain']),
                                        "kode_scheduler": "scraping data user", "record_time": today,
                                        "status": "failed",
                                        "error_message": "response return http error with status code " + str(
                                            respClient.status_code)}
                log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
        except Exception as e:
            error = str(e).replace("'", "")
            log_scheduler_global = {"domain": str(key['sekolah_domain']), "kode_scheduler": "scraping data user",
                                    "record_time": today, "status": "failed", "error_message": error[:200]}
            log_scheduler_global = str(log_scheduler_global).replace("'", '"')
            resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            # log_scheduler = {"domain":key['sekolah_domain'],"record_time": today ,"status":"failed","error_message":error[:100] }
            # log_scheduler =str(log_scheduler).replace("'", '"')
            # respLog = requests.post(log_scheduler, headers=headers, data=log_scheduler)
