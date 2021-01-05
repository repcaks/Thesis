%spark2.ipyspark
# need to be under previous analysis, because it overlayed other charts
heatmap = sns.heatmap(pandas_df_correlation.corr(), annot=True, cmap='YlGnBu')