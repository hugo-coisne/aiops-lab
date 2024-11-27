import pandas as pd

# Charger les fichiers CSV
event_data = pd.read_csv('parsed-hdfs-data.csv')  # Contient BlockId, Date, Time, Event ID
anomaly_labels = pd.read_csv('anomaly_label.csv')  # Contient BlockId et Label

# Groupement par BlockId pour compter les occurrences d'Event ID
event_counts_per_block = event_data.groupby('BlockId')['Event ID'].value_counts().unstack(fill_value=0)

# Renommer les colonnes pour les formater en E1, E2, ..., Ex
event_counts_per_block.columns = [f'E{col}' for col in event_counts_per_block.columns]

# Ajouter les colonnes Date et Time (on prend la première valeur pour chaque BlockId)
date_time = event_data.groupby('BlockId')[['Date', 'Time']].first()

# Ajouter les colonnes Date et Time au DataFrame des événements
event_counts_per_block = event_counts_per_block.merge(
    date_time,
    left_index=True,  # Utiliser BlockId comme index
    right_index=True
)

# Ajouter la colonne "Anomaly" à partir de anomaly_label.csv
event_counts_per_block = event_counts_per_block.merge(
    anomaly_labels[['BlockId', 'Label']],
    left_index=True,  # Utiliser BlockId comme index
    right_on='BlockId',
    how='left'
)

# Convertir la colonne "Label" en binaire : 0 pour Normal, 1 pour Anomaly
event_counts_per_block['Anomaly'] = event_counts_per_block['Label'].apply(lambda x: 1 if x == 'Anomaly' else 0)

# Supprimer la colonne "Label" (optionnel, car elle n'est plus utile)
event_counts_per_block.drop(columns=['Label'], inplace=True)

# Réorganiser les colonnes : BlockId, Date, Time, E1 à Ex, Anomaly
ordered_columns = ['BlockId', 'Date', 'Time'] + [col for col in event_counts_per_block.columns if col.startswith('E')] + ['Anomaly']
event_counts_per_block = event_counts_per_block[ordered_columns]

# Exporter le résultat final dans un fichier CSV
event_counts_per_block.to_csv('HDFS_v1-aggregated.csv', index=False)

# Afficher un aperçu du DataFrame final
print(event_counts_per_block.head())