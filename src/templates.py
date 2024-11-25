import pandas as pd
df = pd.read_csv('csv/event_templates.csv')
print(df.head(), df.shape)
# Filter to keep only the first occurrence of each unique (Event Code, Template) pair
filtered_df = df.groupby("Event", as_index=False, sort=df.index.name).first().drop(columns='Unnamed: 0')
print(filtered_df.columns)

print(filtered_df.head())

# Save to a CSV file
filtered_df.to_csv("csv/filtered_events_pandas.csv", index=False)
print(filtered_df, filtered_df.shape)