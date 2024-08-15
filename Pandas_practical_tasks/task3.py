import pandas as pd
from functools import reduce

file = pd.read_csv(r"aggregated_airbnb_data.csv")


def print_analysis_results(df, msg=''):
    print('\n'.join((msg, df)))


pivot_df = file.pivot_table(values=['price'], index=['neighbourhood_group', 'room_type'])
print_analysis_results(pivot_df.to_string(), 'This is pivot df')
melted_df = (file.melt(id_vars=['neighbourhood', 'last_review', 'neighbourhood_group', 'room_type', 'availability_365'], value_vars=['minimum_nights', 'price', 'number_of_reviews', 'reviews_per_month']))


def availability_status(availability_365):
    if availability_365 < 50:
        return 'Rarely Available'
    elif availability_365 > 200:
        return 'Highly Available'
    else:
        return 'Occasionally Available'


melted_df['availability_status'] = melted_df['availability_365'].apply(availability_status)
print_analysis_results(melted_df.to_string(), 'This is df after melt function with availability status added')
availability_df_mean = melted_df.groupby(['availability_status', 'variable', 'neighbourhood_group', 'room_type'], as_index=False)[['value']].mean().rename(columns={'value': 'mean'})
availability_df_median = melted_df.groupby(['availability_status', 'variable', 'neighbourhood_group', 'room_type'], as_index=False)[['value']].median().rename(columns={'value': 'median'})
availability_df_std = melted_df.groupby(['availability_status', 'variable', 'neighbourhood_group', 'room_type'], as_index=False)[['value']].std().rename(columns={'value': 'std'})
statistics_df = reduce(lambda left, right: pd.merge(left, right, on=['availability_status', 'variable', 'neighbourhood_group', 'room_type'], how='left'), [availability_df_mean, availability_df_median, availability_df_std])

availability_df_mean_av = melted_df.groupby(['availability_status', 'variable'], as_index=False)[['value']].mean().rename(columns={'value': 'mean_av'})
availability_df_median_av = melted_df.groupby(['availability_status', 'variable'], as_index=False)[['value']].median().rename(columns={'value': 'median_av'})
availability_df_std_av = melted_df.groupby(['availability_status', 'variable'], as_index=False)[['value']].std().rename(columns={'value': 'std_av'})
statistics_df = reduce(lambda left, right: pd.merge(left, right, on=['availability_status', 'variable'], how='left'), [statistics_df, availability_df_mean_av, availability_df_median_av, availability_df_std_av])

availability_df_mean_var = melted_df.groupby(['variable'], as_index=False)[['value']].mean().rename(columns={'value': 'mean_var'})
availability_df_median_var = melted_df.groupby(['variable'], as_index=False)[['value']].median().rename(columns={'value': 'median_var'})
availability_df_std_var = melted_df.groupby(['variable'], as_index=False)[['value']].std().rename(columns={'value': 'std_var'})
statistics_df = reduce(lambda left, right: pd.merge(left, right, on=['variable'], how='left'), [statistics_df, availability_df_mean_var, availability_df_median_var, availability_df_std_var])
print_analysis_results(statistics_df.to_string(), 'This is df with calculated descriptive statistics for minimum_nights, price, number_of_reviews, reviews_per_month. '
                                                  'Values are calculated for availability_status, neighbourhood_group, room_type grouping, for availability_status grouping only and without grouping, for all data')
statistics_df = melted_df[['availability_status', 'neighbourhood_group', 'room_type', 'variable', 'value', 'neighbourhood', 'last_review']].merge(statistics_df, on=['availability_status', 'neighbourhood_group', 'room_type', 'variable'], how='left')

statistics_df['last_review'] = pd.to_datetime(statistics_df['last_review'])
print_analysis_results(statistics_df.to_string(), 'This is melted df, with all descriptive statistics added and column transformed to datetime')

time_series_df = statistics_df[['variable', 'last_review', 'value']].copy()
time_series_df['year'] = time_series_df['last_review'].dt.year
time_series_df['month'] = time_series_df['last_review'].dt.month
time_series_df_mean = time_series_df.groupby(['variable', 'year', 'month'], as_index=False)[['value']].mean().rename(columns={'value': 'mean_year_month'})
time_series_df_median = time_series_df.groupby(['variable', 'year', 'month'], as_index=False)[['value']].median().rename(columns={'value': 'median_year_month'})
time_series_df_std = time_series_df.groupby(['variable', 'year', 'month'], as_index=False)[['value']].std().rename(columns={'value': 'std_year_month'})
time_series_df = reduce(lambda left, right: pd.merge(left, right, on=['variable', 'year', 'month'], how='left'), [time_series_df, time_series_df_mean, time_series_df_median, time_series_df_std])

print_analysis_results(time_series_df.to_string(), 'This is descriptive statistics for minimum_nights, price, number_of_reviews, reviews_per_month, grouped for each month of each year separately ')


time_series_df_nei = statistics_df[['variable', 'last_review', 'value', 'neighbourhood_group', 'room_type']].copy()
time_series_df_nei['year'] = time_series_df_nei['last_review'].dt.year
time_series_df_nei['month'] = time_series_df_nei['last_review'].dt.month
time_series_df_nei_mean = time_series_df_nei.groupby(['variable', 'month', 'neighbourhood_group', 'room_type'], as_index=False)[['value']].mean().rename(columns={'value': 'mean_year_month'})
time_series_df_nei_median = time_series_df_nei.groupby(['variable', 'month', 'neighbourhood_group', 'room_type'], as_index=False)[['value']].median().rename(columns={'value': 'median_year_month'})
time_series_df_nei_std = time_series_df_nei.groupby(['variable', 'month', 'neighbourhood_group', 'room_type'], as_index=False)[['value']].std().rename(columns={'value': 'std_year_month'})
time_series_df_nei = reduce(lambda left, right: pd.merge(left, right, on=['variable', 'month', 'neighbourhood_group', 'room_type'], how='left'), [time_series_df_nei, time_series_df_nei_mean, time_series_df_nei_median, time_series_df_nei_std])
print_analysis_results(time_series_df_nei.to_string(), 'This is descriptive statistics for minimum_nights, price, number_of_reviews, reviews_per_month, grouped by month, neighbourhood_group and room_type to detect season changes')
time_series_df_nei = time_series_df_nei.sort_values(by=['neighbourhood_group', 'room_type', 'month'])
time_series_df_nei.to_csv('time_series_airbnb_data.csv', index=False)
