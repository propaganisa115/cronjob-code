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
    urlModul  = "https://" + str(key['sekolah_domain']) + "/api/main/modul"
    if len(str(key['sekolah_domain'])) < 0:
        log_scheduler_global = {"domain": "empty", "kode_scheduler": "scraping modul data", "record_time": today,
                                "status": "failed", "error_message": "sekolah_domain is empty with id school " + str(
                key['id']) + ", you can check in the table sekolah"}
        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
    else:
        try:
            resp_json_modul   = requests.post(urlModul, headers=headersClient,data=filterOnlydate)
            if (resp_json_modul.status_code == 200):
                resp_json_modul = json.loads(resp_json_modul.text)

                if type(resp_json_modul['data'] == "<class 'dict'>"):
                    resp_json_modul['data'] = [resp_json_modul['data']]

                datas_modul = []
                domain = resp_json_modul['sekolah']
                print(domain)


                def ambilData_modul(key):
                    datas_modul.append(
                        {'subject_id': str(key['subject_id']), 'name': str(key['name']), 'id_schedule': str(key['id_schedule']),
                         'guru': str(key['guru']), 'id_modul': str(key['id']), 'title': str(key['title']),
                         'description': str(key['description']), 'file_name': str(key['file_name']),
                         'extension': str(key['extension']), 'tipe_modul': str(key['tipe_modul']),
                         'id_subject': str(key['id_subject']), 'id_guru': str(key['id_guru']), 'id_class': str(key['id_class']),
                         'id_jadwal_pelajaran': str(key['id_jadwal_pelajaran']), 'link': str(key['link']), 'cover': str(key['cover']),
                         'upload': str(key['upload']), 'tipe_link': str(key['tipe_link']), 'deleted': str(key['deleted']),
                         'type': str(key['type']), 'created_at': str(key['created_at']), 'cover_path': str(key['cover_path']),
                         'file_path': str(key['file_path']), 'subject_name': str(key['subject_name']),'sekolah_domain':str(key['sekolah_domain']),'created_at':str(today)})


                with ThreadPoolExecutor(max_workers=None) as exec:
                    fut = [exec.submit(ambilData_modul, key) for key in resp_json_modul['data']]

                urlModul = "https://api.seonindonesia.net/modul/create"
                headersPython = CaseInsensitiveDict()
                headersPython["Accept"] = "application/json"
                headersPython["Authorization"] = "Basic YW5pc2E6YW5pc2E="
                headersPython["Content-Type"] = "application/json"


                def tambahData_modul(key):
                    if key['subject_id']:
                        key['subject_id'] = int(key['subject_id'])
                    else:
                        key['subject_id'] = 0
                        # print(key['subject_id'])
                    if key['id_modul']:
                        key['id_modul'] = int(key['id_modul'])
                    else:
                        key['id_modul'] = 0
                    if key['id_subject']:
                        key['id_subject'] = int(key['id_subject'])
                    else:
                        key['id_subject'] = 0
                    if key['id_class']:
                        key['id_class'] = int(key['id_class'])
                    else:
                        key['id_class'] = 0

                    TambahData_modul = {'subject_id': key['subject_id'], 'name': key['name'],
                                        'id_schedule': key['id_schedule'], 'guru': key['guru'],
                                        'id_modul': key['id_modul'], 'title': key['title'],
                                        'description': key['description'], 'file_name': key['file_name'],
                                        'extension': key['extension'], 'tipe_modul': key['tipe_modul'],
                                        'id_subject': key['id_subject'], 'id_guru': key['id_guru'],
                                        'id_class': key['id_class'], 'id_jadwal_pelajaran': key['id_jadwal_pelajaran'],
                                        'link': key['link'], 'cover': key['cover'], 'upload': key['upload'],
                                        'tipe_link': key['tipe_link'], 'deleted': key['deleted'], 'type': key['type'],
                                        'created_at': key['created_at'], 'cover_path': key['cover_path'],
                                        'file_path': key['file_path'], 'subject_name': key['subject_name'],
                                        'share': 'off','sekolah_domain':key['sekolah_domain'],'created_at':key['created_at']}
                    tambahdatas_modul = str(TambahData_modul).replace("'", '"')
                    resp = requests.post(urlModul, headers=headersPython, data=tambahdatas_modul)

                with ThreadPoolExecutor(max_workers=None) as exec:
                    try:
                        futures = [exec.submit(tambahData_modul, key) for key in datas_modul]
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping modul data", "record_time": today,
                                                "status": "success", "error_message": ""}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
                    except Exception as e:
                        error = str(e).replace("'", "")
                        log_scheduler_global = {"domain": key['sekolah_domain'],
                                                "kode_scheduler": "scraping modul data", "record_time": today,
                                                "status": "failed", "error_message": error[:200]}
                        log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            else:
                log_scheduler_global = {"domain": str(key['sekolah_domain']),
                                        "kode_scheduler": "scraping modul  data", "record_time": today,
                                        "status": "failed",
                                        "error_message": "response return http error with status code " + str(
                                            resp_json_modul.status_code)}
                log_scheduler_global = str(log_scheduler_global).replace("'", '"')
                resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
        except Exception as e:
            error = str(e).replace("'", "")
            log_scheduler_global = {"domain": str(key['sekolah_domain']), "kode_scheduler": "scraping modul data",
                                    "record_time": today, "status": "failed", "error_message": error[:200]}
            log_scheduler_global = str(log_scheduler_global).replace("'", '"')
            resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
            # log_scheduler = {"domain":key['sekolah_domain'],"record_time": today ,"status":"failed","error_message":error[:100] }
            # log_scheduler =str(log_scheduler).replace("'", '"')
            # respLog = requests.post(log_scheduler, headers=headers, data=log_scheduler)
