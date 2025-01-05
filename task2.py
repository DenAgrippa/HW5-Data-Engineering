import functions as f

collection = f.connect_db('jobs')
# collection.insert_many(f.read_csv('data/task_2_item.csv'))

def find_min_max_avg(collection, field_name):
    selection = collection.aggregate([{
        '$group': {
            '_id': None,
            'min': {'$min': f'${field_name}'},
            'max': {'$max': f'${field_name}'},
            'avg_value': {'$avg': f'${field_name}'}
        }
    }])
    return list(selection)

def count_by_profession(collection):
    selection = collection.aggregate([
        { '$group': {
            '_id': '$job',
            'count': { '$sum': 1}
        }}
    ])
    return list(selection)

def stats_by_group(collection, group_field, stats_field):
    selection = collection.aggregate([
        { '$group': {
            '_id': f'${group_field}',
            'min': {'$min': f'${stats_field}'},
            'max': {'$max': f'${stats_field}'},
            'avg': {'$avg': f'${stats_field}'}
        }}
    ])
    return list(selection)

def max_salary_min_age(collection):
    selection = collection.aggregate([
        { '$group': {
            '_id': '$age',
            'salary': {'$max': '$salary'}
        }},
        { '$sort': { '_id': 1} },
        { '$limit': 1}
    ])
    data = []
    return list(selection)   

def min_salary_max_age(collection):
    selection = collection.aggregate([
        { '$group': {
            '_id': '$age',
            'salary': {'$min': '$salary'}
        }},
        { '$sort': { '_id': -1} },
        { '$limit': 1}
    ])
    return list(selection) 



def stats_age_groupby_city_filtered(collection):
    selection = collection.aggregate([
        { '$match': { 'salary': {'$gt': 50_000}}},
        { '$group': {
            '_id': '$city',
            'min': {'$min': '$age'},
            'max': {'$max': '$age'},
            'avg': {'$avg': '$age'}
        }},
        { '$sort': { 'avg': -1}}
    ])
    return list(selection)

def stats_salary_filtered_city_job_age(collection):
    selection = collection.aggregate([
        { '$match': {
            'salary': {'$gt': 50_000, '$lt': 90_000},
            'city': {'$in': ['Осера', 'Таллин', 'Артейхо']},
            'job': {'$in': ['Психолог', 'Медсестра', 'Продавец']},
            '$or': [
                {'age': {'$gt': 18, '$lt': 25}},
                {'age': {'$gt': 50, '$lt': 65}}
            ]
        }},
        { '$group': {
            '_id': 'result',
            'min': {'$min': '$salary'},
            'max': {'$max': '$salary'},
            'avg': {'$avg': '$salary'}
        }}
    ])
    return list(selection)

def custom_query(collection):
    selection = collection.aggregate([
        { '$match': { 'age': {'$gt': 20, '$lt': 35}}},
        { '$group': {
            '_id': '$job',
            'min': {'$min': '$salary'},
            'max': {'$max': '$salary'},
            'avg': {'$avg': '$salary'}
        }},
        { '$sort': { 'max': -1}}
    ])
    return list(selection)    

f.write_to_json(find_min_max_avg(collection, 'salary'), '2', 'find_min_max_avg')
f.write_to_json(count_by_profession(collection), '2', 'count_by_profession')
f.write_to_json(stats_by_group(collection, 'city', 'salary'), '2', 'stats_by_group_city_salary')
f.write_to_json(stats_by_group(collection, 'job', 'salary'), '2', 'stats_by_group_job_salary')
f.write_to_json(stats_by_group(collection, 'city', 'age'), '2', 'stats_by_group_city_age')
f.write_to_json(stats_by_group(collection, 'job', 'age'), '2', 'stats_by_group_job_age')
f.write_to_json(max_salary_min_age(collection), '2', 'max_salary_min_age')
f.write_to_json(min_salary_max_age(collection), '2', 'min_salary_max_age')
f.write_to_json(stats_age_groupby_city_filtered(collection), '2', 'stats_age_groupby_city_filtered')
f.write_to_json(stats_salary_filtered_city_job_age(collection), '2', 'stats_salary_filtered_city_job_age')
f.write_to_json(custom_query(collection), '2', 'custom_query')