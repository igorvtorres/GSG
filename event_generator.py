import hashlib
import json
import logging
import math
import os
from datetime import datetime, date

import requests
from pymongo import MongoClient

logging.basicConfig()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'DEBUG')))
LOGGER.debug(__name__)


def get_bike_incidents(page, per_page):
    try:
        r = requests.get(
            'https://bikeindex.org:443/api/v3/search?page={}&per_page={}&stolenness=all'.format(page, per_page))
        to_return = (r.headers, r.json())
    except Exception as e:
        print(r)
        print(e)
        SystemExit(e)
    return to_return


def get_bike(id):
    try:
        r = requests.get('https://bikeindex.org:443/api/v3/bikes/{}'.format(id))
        to_return = (r.headers, r.json())
    except Exception as e:
        print(r)
        print(e)
        SystemExit(e)
    return to_return


def get_db_bike():
    client = MongoClient('localhost:27017')
    return client.bike


def find_total(db):
    param = db.parameters.find_one()
    if param is None:
        return 0
    else:
        return int(param['last_total'])


def update_total(new_total, db):
    db.parameters.find_one_and_update({"_version": 1}, {"$set": {"last_total": int(new_total)}}, upsert=True)


def resolve_bikes(search_bikes, total_reg):
    total = total_reg
    for search_bike in search_bikes:
        bike = get_bike(search_bike['id'])[1]['bike']
        if bike['stolen']:
            search_bike['stolen_id'] = bike['stolen_record']['id']
            search_bike['stolen_created_at'] = bike['stolen_record']['created_at']
            stolen_record = bike['stolen_record'].copy()
            stolen_record['bike_id'] = bike['id']
            stolen_record['key'] = str(bike['id']) + str(stolen_record['id'])
            stolen_record['stolen_location'] = bike['stolen_location']
            stolen_record['location_found'] = bike['location_found']
            stolen_record['bike_created_at'] = bike['registration_created_at']
            stolen_record['bike_updated_at'] = bike['registration_updated_at']
            save_bike_stolen(stolen_record)
        search_bike['bike_created_at'] = bike['registration_created_at']
        search_bike['bike_updated_at'] = bike['registration_updated_at']
        search_bike['hash'] = hashlib.sha256(json.dumps(search_bike).encode('utf-8')).hexdigest()
        search_bike['current_ts'] = str(datetime.now())
        total = total + 1
        search_bike['internal_id'] = total
        save_bike(bike)
        save_bike_search(search_bike)

    return total


def save_bike_search(record):
    bike_search_file.write(json.dumps(record))
    bike_search_file.write("\n")
    db.bikeSearch.insert_one(record)


def save_bike(record):
    bike_file.write(json.dumps(record))
    bike_file.write("\n")
    db.bike.insert_one(record)


def save_bike_stolen(record):
    bike_stolen_file.write(json.dumps(record))
    bike_stolen_file.write("\n")
    db.bikeStolen.insert_one(record)


def open_files():
    global bike_search_file
    global bike_file
    global bike_stolen_file

    bike_search_path = "raw_data/bike-search/day={}/bike-search-{}".format(date.today().strftime("%Y%m%d"),
                                                                           datetime.now().timestamp())
    os.makedirs(os.path.dirname(bike_search_path), exist_ok=True)
    bike_search_file = open(bike_search_path, "w")

    bike_path = "raw_data/bike/day={}/bike-{}".format(date.today().strftime("%Y%m%d"), datetime.now().timestamp())
    os.makedirs(os.path.dirname(bike_path), exist_ok=True)
    bike_file = open(bike_path, "w")

    bike_stolen_path = "raw_data/bike-stolen/day={}/bike-stolen-{}".format(date.today().strftime("%Y%m%d"),
                                                                           datetime.now().timestamp())
    os.makedirs(os.path.dirname(bike_stolen_path), exist_ok=True)
    bike_stolen_file = open(bike_stolen_path, "w")


if __name__ == "__main__":

    db = get_db_bike()
    update_total(625000, db)
    last_total = find_total(db)

    req_bike = get_bike_incidents("1", "1")
    total_init = int(req_bike[0]['Total'])
    num_rec = total_init - last_total
    while num_rec != 0:
        print("num_rec: {}".format(num_rec))
        open_files()

        miss_record = num_rec % 100
        page = math.floor(num_rec / 100)
        if miss_record != 0:
            print("MOD: {}".format(miss_record))
            first_bike = get_bike_incidents(str(page + 1), str(miss_record))
            bikes = first_bike[1]['bikes']
            last_total = resolve_bikes(bikes, last_total)
            # last_total = last_total + saved_bikes
            update_total(last_total, db)
        for pag in range(page, 0, -1):
            print("page: {}".format(pag))
            req_bike = get_bike_incidents(str(pag), "100")
            new_total = int(req_bike[0]['Total'])
            if new_total != total_init:
                per_page = new_total - total_init
                if per_page < 0:
                    print("ERROR: total records smaller than before - Last: {} - Before: {}".format(new_total,
                                                                                                    total_init))
                    break
                miss_bike = get_bike_incidents(str(pag + 1), str(per_page))
                print("page: {} - total_req: {} - total_init: {}".format(pag, new_total, total_init))
                bikes = req_bike[1]['bikes'] + miss_bike[1]['bikes']
                last_total = resolve_bikes(bikes, last_total)
                total_init = new_total
                update_total(last_total, db)
                break
            else:
                bikes = req_bike[1]['bikes']
                last_total = resolve_bikes(bikes, last_total)
                update_total(last_total, db)
            print("last_total: {}".format(last_total))
        bike_search_file.close()
        bike_file.close()
        bike_stolen_file.close()
        num_rec = total_init - last_total
        print("last_total: {}".format(last_total))
    mongoCount = db.bikeSearch.count_documents({})
    print(mongoCount)
