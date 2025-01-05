import pymongo
import functions as f


collection = f.connect_db('jobs')
# collection.insert_many(f.read_text("data/task_1_item.text"))

def sort_by_salary(collection):
    return list(collection.find(limit = 10).sort({'salary': pymongo.DESCENDING}))

def filter_by_age(collection):
    return list(collection
                .find({'age': {"$lt": 30}}, limit = 15)
                .sort({'salary': pymongo.DESCENDING}))

def filter_by_city_job(collection):
    return list(collection
                .find({
                    'city': "Будапешт",
                    'job': {"$in": ['Архитектор', 'Косметолог', 'Повар']}
                },limit = 10)
                .sort({'age': pymongo.ASCENDING}))

def filter_by_age_yaer_salary(collection):
    return (collection
                .count_documents({
                    'age': {'$gt': 20, '$lt': 40},
                    'year': {'$lte': 2022, '$gte': 2019},
                    'or': [
                        {'salary': {'gt': 50000, '$lte': 75000}},
                        {'salary': {'gt': 125000, 'lt': 150000}}
                    ]
                }))

f.write_to_json(sort_by_salary(collection), '1', 'sort_by_salary')
f.write_to_json(filter_by_age(collection), '1', 'filter_by_age')
f.write_to_json(filter_by_city_job(collection), '1', 'filter_by_city_job')
f.write_to_json(filter_by_age_yaer_salary(collection), '1', 'filter_by_age_yaer_salary')