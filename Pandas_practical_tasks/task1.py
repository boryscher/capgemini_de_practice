import pandas as pd
import numpy as np


file = pd.read_csv(r"AB_NYC_2019.csv")


def print_dataframe_info(df, msg=''):
    print('\n'.join((msg, df)))


print(file.head())
print(file.info())

print_dataframe_info(file.isnull().sum().to_string(), 'Info about dataframe when missing values filled')
file.fillna(value={'name': 'Unknown', 'host_name': 'Unknown', 'last_review': pd.NaT}, inplace=True)
print_dataframe_info(pd.DataFrame(file.info()).to_string(), 'Info about dataframe when missing values filled')


def _set_price_category(price):
    if price < 100:
        return 'Low'
    elif price >= 300:
        return 'High'
    else:
        return 'Medium'


set_price_category = np.vectorize(_set_price_category)
file['price_category'] = set_price_category(file['price'])
print_dataframe_info(str(file['price_category'].isnull().sum()), 'Check if every value is filled in price category')


def _set_length_of_stay_category(nights):
    if nights <= 3:
        return 'short-term'
    elif nights > 14:
        return 'long-term'
    else:
        return 'medium-term'


set_length_of_stay_category = np.vectorize(_set_length_of_stay_category)
file['length_of_stay_category'] = set_length_of_stay_category(file['minimum_nights'])
print_dataframe_info(str(file['length_of_stay_category'].isnull().sum()), 'Check if every value is filled in length of stay category')

print_dataframe_info(str(file['name'].isnull().sum()), 'Check if we have empty values in name column')
print_dataframe_info(str(file['host_name'].isnull().sum()), 'Check if we have empty values in host_name column')
print_dataframe_info(str(file['last_review'].isnull().sum()), 'Check if we have empty values in last_review column')

print_dataframe_info(file[file['price'] == 0].to_string(), 'Find price equal to 0')
file = file[file['price'] != 0]
print_dataframe_info(file[file['price'] == 0].to_string(), 'Check if 0 price records are removed')

file.to_csv('cleaned_airbnb_data.csv', index=False)
