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
headersClient["Authorization"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiU0VPTkFETTAwMSIsImZ1bGxuYW1lIjoiU2VvbiBBZG1pbiIsImVtYWlsIjoic2VvbmFkbWluQHNlb24uY29tIiwidHlwZSI6InN1cGVyX2FkbWluIiwiQVBJX1RJTUUiOjE2MjcwMjM3Mzl9.4HiC4XnwiQ6CpfpCg_IVqVQeLr0pwqT-pIEjRPb6dvQ"
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
    urlActivity_user = "https://" + str(key['sekolah_domain']) + "/api/main/activity"
    if len(str(key['sekolah_domain'])) < 0:
        log_scheduler_global = {"domain": "empty", "kode_scheduler": "scraping activity user data", "record_time": today,
                                "status": "failed", "error_message": "sekolah_domain is empty with id school " + str(
                key['id']) + ", you can check in the table sekolah"}
        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
    else:
        try:
            resp_json_activityuser = requests.post(urlActivity_user, headers=headersClient)
            if (resp_json_activityuser.status_code == 200):
                resp_json_activityuser = json.loads(resp_json_activityuser.text)
                datas_activityuser = []


                def ambilData_activityuser(key):
                    datas_activityuser.append(
                        {'id_activity': key['id_activity'], 'tipe_user': key['tipe_user'], 'user_id': key['user_id'],
                         'aktivitas': key['aktivitas'], 'id_ref_activity': key['id_ref_activity'],
                         'id_jadwal_pelajaran': key['id_jadwal_pelajaran'], 'datetime': key['datetime'],
                         'type_activity': key['type_activity'], 'tb_reff': key['tb_reff'], 'id_user': key['id_user'],
                         'fullname': key['fullname'], 'foto_profile': key['foto_profile'], 'cover': key['cover'],
                         'level_user': key['level_user'], 'deskripsi': key['deskripsi']})


                with ThreadPoolExecutor(max_workers=None) as exec:
                    fut = [exec.submit(ambilData_activityuser, key) for key in resp_json_activityuser['activities']]

                urlActivityuser = "https://api.seonindonesia.net/activity_user/create"
                headersPython = CaseInsensitiveDict()
                headersPython["Accept"] = "application/json"
                headersPython["Authorization"] = "Basic YW5pc2E6YW5pc2E="
                headersPython["Content-Type"] = "application/json"


                def tambahData_activityuser(key):
                    TambahData_activityuser = {'id_activity': key['id_activity'], 'tipe_user': key['tipe_user'],
                                               'user_id': key['user_id'], 'aktivitas': key['aktivitas'],
                                               'id_ref_activity': key['id_ref_activity'],
                                               'id_jadwal_pelajaran': key['id_jadwal_pelajaran'],
                                               'datetime': key['datetime'], 'type_activity': key['type_activity'],
                                               'tb_reff': key['tb_reff'], 'id_user': key['id_user'],
                                               'fullname': key['fullname'], 'foto_profile': key['foto_profile'],
                                               'cover': key['cover'], 'level_user': key['level_user'],
                                               'deskripsi': key['deskripsi']}
                    tambahdatas_activityuser = str(TambahData_activityuser).replace("'", '"')
                    resp = requests.post(urlActivityuser, headers=headersPython, data=tambahdatas_activityuser)

                with ThreadPoolExecutor(max_workers=None) as exec:
                    try:
                        futures = [exec.submit(tambahData_activityuser, key) for key in datas_activityuser]
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping activity user data", "record_time": today,
                                                "status": "success", "error_message": ""}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
                    except Exception as e:
                        error = str(e).replace("'", "")
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping activity user data", "record_time": today,
                                                "status": "sucess", "error_message": error[:200]}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            else:
                log_scheduler_global = {"domain": str(key['sekolah_domain']),
                                        "kode_scheduler": "scraping activity user data", "record_time": today,
                                        "status": "failed",
                                        "error_message": "response return http error with status code " + str(
                                            resp_json_activityuser.status_code)}
                log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
        except Exception as e:
            error = str(e).replace("'", "")
            log_scheduler_global = {"domain": str(key['sekolah_domain']), "kode_scheduler": "scraping activity user data",
                                    "record_time": today, "status": "failed", "error_message": error[:200]}
            log_scheduler_global = str(log_scheduler_global).replace("'", '"')
            resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            # log_scheduler = {"domain":key['sekolah_domain'],"record_time": today ,"status":"failed","error_message":error[:100] }
            # log_scheduler =str(log_scheduler).replace("'", '"')
            # respLog = requests.post(log_scheduler, headers=headers, data=log_scheduler)
