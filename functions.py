from pymongo import MongoClient
import msgpack
import csv
import json

def connect_db(collection_name):
    client = MongoClient()
    db = client["db_for_hw"]
    if collection_name == 'jobs':
        return db.jobs
    if collection_name == 'test':
        return db.test
    if collection_name == 'task4':
        return db.task4

def data_type_conversion(data):
    for item in data:
        for key, value in item.items():
            if "." in value:
                try: item[key] = float(value)
                except ValueError: pass
            else:
                try: item[key] = int(value)
                except ValueError: pass
            if value.lower() == 'yes':
                item[key] = True
            if value.lower() == 'no':
                item[key] = False
    return data

def read_text(path):
    with open(path, "r", encoding="utf-8") as file:
        raw_data = file.read().strip().split("=====")
        data = []
        for entry in raw_data:
            if entry == '': continue

            entry_data = entry.strip().split("\n")
            item = {}
            for prop in entry_data:
                key, value = prop.split('::')
                item[key] = value
            data.append(item)
        return data_type_conversion(data)

def read_msgpack(path):
    with open(path, "rb") as file:
        return msgpack.load(file)
    
def read_csv(path, delimiter=';'):
    with open(path, "r", encoding="utf-8") as file:
        csv_data = csv.DictReader(file, delimiter=delimiter)
        data = []
        for row in csv_data:
            data.append(row)
    return data_type_conversion(data)

def read_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def write_to_json(output, task_number, file_id):
    print(output)
    if type(output) is list:
        data = []
        for item in output:
            if '_id' in item:
                del item['_id']
                print(item)
            data.append(item)
    else:
        data = output
    with open(f'result/task_{task_number}/output_{file_id}.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)
    
