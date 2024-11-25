import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
df_aggregated= pd.read_csv('csv/BGL-aggregated2.csv')

import dask.dataframe as dd
ddf = dd.from_pandas(df_aggregated.drop(columns=['Window Start','Window End']), npartitions=10)
correlation_matrix = ddf.corr().compute()

# Plot the heatmap using Seaborn
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)

# Add titles and labels
plt.title("Correlation Matrix")
plt.show()