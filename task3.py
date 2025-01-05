import functions as f

collection = f.connect_db('jobs')
collection.insert_many(f.read_msgpack('data/task_3_item.msgpack'))

def remove_based_on_salary(collection):
    return collection.delete_many({
        '$or': [
            {'salary': {'$lt': 25_000} },
            {'salary': {'$gt': 175_000}}
        ]
    })

def increment_age(collection):
    return collection.update_many(
        {},
        {'$inc': {'age': 1}}
    )

def raise_pay_by_job(collection):
    return collection.update_many(
        {'job': {'$in': ['Инженер', 'Водитель', 'Косметолог', 'Медсестра']}},
        {'$mul': {'salary': 1.05}}
    )

def raise_pay_by_city(collection):
    return collection.update_many(
        {'city': {'$in': ['Барселона', 'Хельсинки', 'Загреб']}},
        {'$mul': {'salary': 1.07}}
    )

def raise_pay_custom(collection):
    return collection.update_many(
        {
            'city': {'$in': ['Ташкент', 'Камбадос', 'Валенсия', 'Белград']},
            'job': {'$in': ['Бухгалтер', 'Психолог', 'Менеджер', 'Архитектор']},
            'age': {'$gt': 25, '$lt': 50}
        },
        {'$mul': {'salary': 1.10}}
    )

def delete_custom(collection):
    return collection.delete_many({
        'city': {'$in': ['Будапешт', 'Мартоса']},
        'age': {'$lt': 50}
    })
print(remove_based_on_salary(collection))
print(increment_age(collection))
print(raise_pay_by_job(collection))
print(raise_pay_by_city(collection))
print(raise_pay_custom(collection))
print(delete_custom(collection))