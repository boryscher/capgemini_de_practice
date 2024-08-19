import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


file = pd.read_csv(r"AB_NYC_2019.csv")

barplot_data = file.groupby(['neighbourhood_group'])['name'].size().reset_index()

plt.bar(barplot_data['neighbourhood_group'], barplot_data['name'], color=('red', 'blue', 'green', 'orange', 'yellow'))

for i in range(len(barplot_data['neighbourhood_group'])):
    plt.text(i, barplot_data['name'][i], barplot_data['name'][i], ha = 'center')

plt.title("Neighborhood Distribution of Listings")

plt.xlabel("Neighborhood Group")
plt.ylabel("Number of Listings")

plt.show()
plt.savefig(r"Neighborhood Distribution of Listings.png")


neighbourhood_groups = file['neighbourhood_group'].unique().tolist()
colors = ['grey', 'blue', 'green', 'orange', 'yellow']
boxplot_data = [file['price'].loc[(file['neighbourhood_group'] == neighbourhood_group) & (file['price'] < 1000)].tolist() for neighbourhood_group in neighbourhood_groups]

plt.ylabel('Price')
plt.xlabel('Neighborhood Group')
plt.title("Neighborhood Distribution of Price")
plot = plt.boxplot(boxplot_data, patch_artist=True, tick_labels=neighbourhood_groups, flierprops=dict(marker='o', markerfacecolor='r'))

for patch, color in zip(plot['boxes'], colors):
    patch.set_facecolor(color)

plt.show()
plt.savefig(r"Neighborhood Distribution of Price.png")

group_barplot_data = file.groupby(['neighbourhood_group', 'room_type'])['availability_365'].describe().reset_index()
group_barplot_data = group_barplot_data.pivot_table(values=['mean', 'std'], columns=['room_type'], index=['neighbourhood_group'])
group_barplot_data_mean = group_barplot_data[list(filter(lambda x: x[0] == 'mean', group_barplot_data.columns.tolist()))]
group_barplot_data_std = group_barplot_data[list(filter(lambda x: x[0] == 'std', group_barplot_data.columns.tolist()))]
group_barplot_data_mean.columns = group_barplot_data_mean.columns.droplevel(0)
group_barplot_data_std.columns = group_barplot_data_std.columns.droplevel(0)
group_barplot_data_mean.plot(kind='bar',
         rot=0,
        title='Room Type vs. Availability', yerr=group_barplot_data_std)

plt.xlabel("Neighborhood Group")
plt.ylabel("Average Availability")

plt.show()
plt.savefig(r"Room Type vs. Availability.png")

room_types = file['room_type'].unique().tolist()
scatterplot_data = file[['price', 'number_of_reviews', 'room_type']].copy()
sns.lmplot(x="price", y="number_of_reviews", hue="room_type", data=scatterplot_data, ci=None)
plt.xlabel("Price")
plt.ylabel("Number Of Reviews")
plt.title("Correlation Between Price and Number of Reviews")
plt.show()
plt.savefig(r"Correlation Between Price and Number of Reviews.png")

for neighbourhood_group in neighbourhood_groups:
    lineplot_data = file[file['neighbourhood_group'] == neighbourhood_group][['last_review', 'number_of_reviews']].copy()
    lineplot_data = lineplot_data[~lineplot_data['last_review'].isnull()]
    lineplot_data['last_review'] = pd.to_datetime(lineplot_data['last_review'])
    lineplot_data = lineplot_data.rolling(3, on='last_review').mean()
    plt.plot(lineplot_data['last_review'], lineplot_data['number_of_reviews'])
plt.xlabel("Last Review")
plt.ylabel("Number Of Reviews")
plt.title("Time Series Analysis of Reviews")

plt.legend(labels=neighbourhood_groups)
plt.show()
plt.savefig(r"Time Series Analysis of Reviews.png")

heatmap_data = file[['neighbourhood_group', 'price', 'availability_365']].copy()
heatmap_data_pivot = heatmap_data.groupby(['neighbourhood_group'])[['price', 'availability_365']].corr().reset_index()
heatmap_data_pivot_vals = heatmap_data_pivot[heatmap_data_pivot['level_1'] == 'availability_365']['price'].tolist()
heatmap_data_pivot_cols = heatmap_data_pivot[heatmap_data_pivot['level_1'] == 'availability_365']['neighbourhood_group'].tolist()
# Not understand the task clearly, do we need 5 heatmaps for each group or heatmap with correlation between correlation of proce and availability?
# Not finished task

for room_type in room_types:
    stack_plot_data = file[file['room_type'] == room_type][['number_of_reviews', 'neighbourhood_group']].copy()
    stack_plot_data = stack_plot_data.groupby(['neighbourhood_group'])['number_of_reviews'].sum().reset_index()
    plt.bar(stack_plot_data['neighbourhood_group'], stack_plot_data['number_of_reviews'])

plt.xlabel("Neighbourhood Group")
plt.ylabel("Number Of Reviews")
plt.title("Room Type and Review Count Analysis")

plt.legend(labels=room_types)
plt.show()
plt.savefig(r"Room Type and Review Count Analysis.png")
