import pandas as pd

file = pd.read_csv(r"cleaned_airbnb_data.csv")


def print_grouped_data(df, msg=''):
    print('\n'.join((msg, df)))


def filter_by_group(df, group):
    return df.loc[df['neighbourhood_group'] == group]


def filter_by_price(df):
    return df.loc[(df['price'] > 100) & (df['number_of_reviews'] > 10)]


manhattan = filter_by_group(file, 'Manhattan')
brooklyn = filter_by_group(file, 'Brooklyn')
print_grouped_data(manhattan.to_string(), 'This is data for Manhattan')
print_grouped_data(brooklyn.to_string(), 'This is data for Brooklyn')
file = filter_by_price(file)

file1 = file[['neighbourhood_group', 'price', 'minimum_nights', 'number_of_reviews', 'price_category', 'availability_365']]
print_grouped_data(file1.to_string(), 'This is file ready to grouping, with columns removed and data filtered')
grouped = file.groupby(['neighbourhood_group', 'price_category'])[['price', 'minimum_nights', 'number_of_reviews', 'availability_365']].mean().rename(columns={'price': 'average_price', 'number_of_reviews': 'average_number_of_reviews'})
print_grouped_data(grouped.to_string(), 'This is grouped data with average price and average_number_of_reviews, also average minimum nights and average availability_365')
sorted_data = file.sort_values(by=['price', 'number_of_reviews'], ascending=[False, True])
print_grouped_data(sorted_data.to_string(), 'This is sorted dataset by price and number_of reviews')
sorted_data_size = file.groupby(['neighbourhood'], as_index=False)['name'].size().reset_index()
print_grouped_data(sorted_data_size.to_string(), 'This is dataset with number of neighborhoods per each group')

# data merged together to single dataframe
sorted_data = sorted_data.merge(grouped.reset_index()[['neighbourhood_group', 'price_category', 'average_price']], on=['neighbourhood_group', 'price_category'], how='left')
sorted_data = sorted_data.merge(sorted_data_size, on=['neighbourhood'], how='left')

# added rank for neighborhoods
sorted_data['rank'] = sorted_data.groupby(['average_price', 'size'])['neighbourhood'].rank(method='dense')
print_grouped_data(sorted_data.to_string(), 'This is final grouped dataset')
sorted_data.to_csv('aggregated_airbnb_data.csv', index=False)
