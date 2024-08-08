import numpy as np


# Function to print output
def print_array(array, msg=''):
    print(' '.join((msg, np.array_str(array))))


# Function to generate random array with dim 6 X length
def generate_array(length):
    transaction_id = np.random.randint(10000000, size=length)
    user_id = np.random.randint(2, size=length)
    product_id = np.random.randint(1000, size=length)
    quantity = np.random.randint(3, size=length)
    price = np.random.rand(length) * 1000
    days = np.arange(-365, 365)
    year_ago = np.datetime64('2023-08-05')
    timestamp = np.array([np.datetime64(year_ago + np.random.choice(days)) for _ in range(length)])
    return np.reshape(np.array((transaction_id, user_id, product_id, quantity, price, timestamp)), (6, length))


# Function to get total revenue for all transactions
def total_revenue(array):
    return np.array(np.dot(array[3, :].astype('int32'), array[4, :].astype('float64')))


# Function to get number of unique users
def unique_users(array):
    return np.array(np.unique(array[1, :]).size)


# Function to get most purchased product
def most_purchased(array):
    return array[2, np.where(array[3, :].astype('int32') == np.max(array[3, :].astype('int32')))[0]]


# Function to convert price from float to int
def price_to_int(array):
    array[4, :] = array[4, :].astype('int32')
    return array


# Function to check type of particular row
def check_type(array, row):
    return np.array(array[row].dtype)


# Function to create product quantity array
def product_quantity_array(array):
    return array[2:4, :]


# Function to count transactions per user
def count_unique(array):
    return np.array(np.unique(array[1, :], return_counts=True))


# Function to mask array removing 0 quantity
def remove_zeros(array):
    return np.delete(array, np.where(array[3, :].astype('int32') == 0)[0].tolist(), 1)


# Function to increase price (float price format, e.g. *1.05 means increase to 5%)
def increase_price(array, price):
    array[4, :] = array[4, :] * price
    return array


# Function to filter transactions with quantity > 1
def filter_quantity(array):
    return np.delete(array, np.where(arr[3, :].astype('int32') <= 1)[0].tolist(), 1)


# Function to keep only transactions in provided period
def slice_period(array, period):
    start, end = period
    return np.delete(array, np.where(~((array[5, :] >= np.datetime64(start)) & (array[5, :] <= np.datetime64(end))))[0].tolist(), 1)


# Function to get revenue in two different periods
def compare_revenue(array, period_1, period_2):
    arr1 = slice_period(array, period_1)
    arr2 = slice_period(array, period_2)
    return np.vstack((total_revenue(arr1), total_revenue(arr2)))


# Function to extract all transactions for user
def user_transactions(array, user_id):
    return np.delete(array, np.where(arr[1, :].astype('int32') != user_id)[0].tolist(), 1)[0, :]


# Function to get 5 top transactions in terms quantity*price
def top_products(array):
    idx = np.argpartition(array[3, :] * array[4, :], len(array) - 5)[-5:]
    return array[:, idx]


arr = generate_array(10)
print_array(arr, 'This is random generated array')
print_array(total_revenue(arr), 'Total revenue of all transactions')
print_array(unique_users(arr), 'The number of unique users')
print_array(most_purchased(arr), 'The most purchased product')
print_array(price_to_int(arr), 'Price converted to int')
print_array(check_type(arr, 4), 'Check type')
print_array(product_quantity_array(arr), 'Product quantity array created')
print_array(count_unique(arr), 'Check count of trasnsactions per user')
print_array(remove_zeros(arr), '0 quanity transactions are removed')
print_array(increase_price(arr, 1.05), 'Price increased to 5%')
print_array(filter_quantity(arr), 'Only 2 and more quantity available')
print_array(slice_period(arr, ('2024-01-01', '2024-08-05')), 'See only data for selected period')
print_array(compare_revenue(arr, ('2024-01-01', '2024-08-05'), ('2023-01-01', '2023-12-31')), 'See revenue for 2 periods')
print_array(user_transactions(arr, 1), 'See all transactions for this user')
print_array(top_products(arr), 'See top 5 products array')

