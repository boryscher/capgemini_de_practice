import pandas as pd
from bokeh.models import ColumnDataSource
from bokeh.palettes import Bright4, Bright3
from bokeh.plotting import figure, show, output_file, save
from bokeh.transform import factor_cmap

titanic = pd.read_csv(r"Titanic-Dataset.csv")

titanic["Age"] = titanic["Age"].fillna(round(titanic["Age"].median()))
titanic["Embarked"] = titanic["Embarked"].fillna(round(titanic["Embarked"].mode()))
titanic.drop(['Cabin'], axis=1, inplace=True)


def age_group(age):
    if age < 18:
        return 'Child'
    elif age < 40:
        return 'Young Adult'
    elif age < 60:
        return 'Adult'
    else:
        return 'Elder'


titanic['Age Group'] = titanic['Age'].apply(age_group)
titanic_size = titanic.groupby(['Age Group'])['Survived'].size().reset_index()
titanic_surv = titanic.groupby(['Age Group'])['Survived'].sum().reset_index().rename(columns={'Survived': 'Yes'})
titanic_yes = titanic_size.merge(titanic_surv, on=['Age Group'], how='inner')
titanic_yes['SurvivalRate'] = round(titanic_yes['Yes'] / titanic_yes['Survived'] * 100, 2)
titanic = titanic.merge(titanic_yes[['Age Group', 'SurvivalRate']], on=['Age Group'], how='inner')

source = ColumnDataSource(data=dict(x=titanic['Age Group'].unique().tolist(), top=titanic['SurvivalRate'].unique().tolist(), color=Bright4))

p = figure(x_range=titanic['Age Group'].unique(), title="Age Group Survival", x_axis_label="Age Group", y_axis_label="Survival")
p.vbar(x='x', top='top', width=0.9, color='color', legend_field='x', source=source)
p.xgrid.grid_line_color = None
p.legend.orientation = "horizontal"
p.legend.location = "top_center"
output_file(r'Age Group Survival.html')
save(p)
show(p)

titanic_size_class = titanic.groupby(['Pclass', 'Sex'])['Survived'].size().reset_index()
titanic_surv_class = titanic.groupby(['Pclass', 'Sex'])['Survived'].sum().reset_index().rename(columns={'Survived': 'Yes'})
titanic_yes_class = titanic_size_class.merge(titanic_surv_class, on=['Pclass', 'Sex'], how='inner')
titanic_yes_class['SurvivalRate'] = round(titanic_yes_class['Yes'] / titanic_yes_class['Survived'] * 100, 2)
titanic_yes_class['Pclass'] = titanic_yes_class['Pclass'].astype('str')
group = titanic_yes_class[['Pclass', 'Sex', 'SurvivalRate']].groupby(['Pclass', 'Sex'])
source = ColumnDataSource(data=group)
index_cmap = factor_cmap('Pclass_Sex', palette=Bright3, factors=sorted(titanic_yes_class['Sex'].unique()), end=1)

p = figure(width=800, height=300, title="Class and Gender Survival",
           x_range=group, toolbar_location=None, tooltips=[("SurvivalRate", "@SurvivalRate_mean"), ("Pclass, Sex", "@Pclass_Sex")])
p.vbar(x='Pclass_Sex', top='SurvivalRate_mean', width=1, source=group,
       line_color="white", fill_color=index_cmap)
p.xgrid.grid_line_color = None

p.xaxis.axis_label = "Gender grouped by class"
output_file(r'Class and Gender Survival.html')
save(p)
show(p)


# Get the unique departments and assign a color to each department
scatter_data = titanic[['Fare', 'Survived', 'Pclass']].copy()
scatter_data['Pclass'] = scatter_data['Pclass'].astype('str')

p = figure(title="Fare vs. Survival Through Classes",
           x_axis_label='Fare',
           y_axis_label='Survived', tooltips=[("Fare", "@Fare"), ("Class", "@Pclass")])

source = ColumnDataSource(data=scatter_data)
classes = scatter_data['Pclass'].unique().tolist()
color_map = factor_cmap(field_name='Pclass', palette=Bright3, factors=classes)

p.scatter('Fare', 'Survived', color=color_map, legend_field='Pclass', source=source)
output_file(r'Fare & Survival.html')
save(p)
show(p)
