import functions as f

collection = f.connect_db('test')
collection.insert_many(f.read_json('data/shopping_trends_1.json'))
collection.insert_many(f.read_csv('data/shopping_trends_2.csv', delimiter=','))

# Выборка

def filter_by_age_gender(collection):
    return list(collection
                .find({
                    'Age': {"$lt": 30},
                    'Gender': 'Male'
                }, limit = 5)
                .sort({'Review Rating': -1}))

def filter_by_gender_item(collection):
    return list(collection
                .find({
                    'Item Purchased': 'T-shirt',
                    'Gender': 'Male'
                })
                .sort({'Review Rating': -1}))

def filter_by_location_age_gender_color(collection):
    return list(collection
                .find({
                    'Location': {'$in': ['Texas', 'Florida']},
                    'Age': {'$gt': 30},
                    'Gender': 'Male',
                    'Color': 'Pink'
                })
                .sort({'Review Rating': -1}))

def filter_by_rating_previouspurchases(collection):
    return list(collection
                .find({
                    '$or': [
                        {'Review Rating': {'$lt': 2.0}},
                        {'Review Rating': {'$gt': 4.5}}
                    ],
                    'Previous Purchases': {'$lt': 5}
                }, limit = 10))

def filter_by_payment_method(collection):
    return list(collection
                .find({
                    'Preferred Payment Method': 'Bank Transfer'
                }, limit=3))    



# Агрегация
def group_by_payment_count(collection):
    data = collection.aggregate([{
        '$group': {
            '_id': '$Preferred Payment Method',
            'count': {'$sum': 1}
        }
    }])
    return list(data)
def group_by_seson_max_amount(collection):
    data = collection.aggregate([{
        '$group': {
            '_id': '$Season',
            'max_purchase': {'$max': '$Purchase Amount (USD)'}
        }
    }])
    return list(data)    

def group_by_category_count(collection):
    data = collection.aggregate([{
        '$group': {
            '_id': '$Category',
            'count': {'$sum': 1}
        }
    }])
    return list(data)    

def group_by_gender_jacket(collection):
    data = collection.aggregate([{
        '$match': {'Item Purchased': 'Jacket'}
    }, {
        '$group': {
            '_id': '$Gender',
            'count': {'$sum': 1}
        }
    }])
    return list(data)  

def group_by_size_avg_age(collection):
    data = collection.aggregate([{
        '$group': {
            '_id': '$Size',
            'avg_age': {'$avg': '$Age'}
        }
    }])
    return list(data)    



# Обновление
def decrease_purchase(collection):
    return collection.update_many(
        {'Frequency of Purchases': 'Weekly'},
        {'$mul': {'Purchase Amount (USD)': 0.95}}
    )

def increase_age(collection):
    return collection.update_many(
        {},
        {'$inc': {'Age': 1}}
    )
 
def delete_Ohio(collection):
    return collection.delete_many({
        'Location': 'Ohio'
    })

def make_it_english(collection):
    return collection.update_many(
        {'Season': 'Fall'},
        {'$set': {'Season': 'Autumn'}}
    )
    
def start_id_from_zero(collection):
    return collection.update_many(
        {},
        {'$inc': {'Customer ID': -1}}
    )

f.write_to_json(filter_by_age_gender(collection), '4', 'filter_by_age_gender')
f.write_to_json(filter_by_gender_item(collection), '4', 'filter_by_gender_item')
f.write_to_json(filter_by_location_age_gender_color(collection), '4', 'filter_by_location_age_gender_color')
f.write_to_json(filter_by_rating_previouspurchases(collection), '4', 'filter_by_rating_previouspurchases')
f.write_to_json(filter_by_payment_method(collection), '4', 'filter_by_payment_method')

f.write_to_json(group_by_payment_count(collection), '4', 'group_by_payment_count')
f.write_to_json(group_by_seson_max_amount(collection), '4', 'group_by_seson_max_amount')
f.write_to_json(group_by_category_count(collection), '4', 'group_by_category_count')
f.write_to_json(group_by_gender_jacket(collection), '4', 'group_by_gender_jacket')
f.write_to_json(group_by_size_avg_age(collection), '4', 'group_by_size_avg_age')

print(decrease_purchase(collection))
print(increase_age(collection))
print(delete_Ohio(collection))
print(make_it_english(collection))
print(start_id_from_zero(collection))