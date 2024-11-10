import pandas as pd

df = pd.read_csv('parsed-hdfs-data.csv')

df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str), format='%y%m%d %H%M%S', errors='coerce')

print("Lignes avec des dates mal formées :")
print(df[df['Datetime'].isna()])

df = df.sort_values(by='Datetime')

def split_and_aggregate_by_cluster(df, time_interval='30T', error_threshold=5, anomaly_clusters=None):
    """
    Divise le DataFrame en morceaux temporels et compte les occurrences de chaque Cluster ID dans chaque morceau.
    Ajoute une colonne 'Anomaly' basée sur la fréquence de certains 'Cluster ID' ou niveaux de log.

    :param df: Le DataFrame contenant les données de logs.
    :param time_interval: La fréquence pour découper (par exemple, '30T' pour 30 minutes).
    :param error_threshold: Le nombre de certains clusters qui déclenche l'étiquetage d'anomalie.
    :param anomaly_clusters: Une liste d'IDs de clusters considérés comme anormaux.
    :return: Un nouveau DataFrame où chaque ligne est un intervalle de temps et chaque colonne est un compte de Cluster ID,
             avec une colonne 'Anomaly'.
    """
    
    df.set_index('Datetime', inplace=True)

    cluster_counts = df.groupby([pd.Grouper(freq=time_interval), 'Cluster ID']).size().unstack(fill_value=0)

    cluster_counts['Anomaly'] = '0'

    for index, row in cluster_counts.iterrows():
        if anomaly_clusters:
            if row[anomaly_clusters].sum() >= error_threshold:
                cluster_counts.at[index, 'Anomaly'] = '1'
        else:
            if row.sum() >= error_threshold:
                cluster_counts.at[index, 'Anomaly'] = '1'

    if anomaly_clusters:
        cluster_counts.drop(columns=anomaly_clusters, inplace=True)

    return cluster_counts

time_interval = '30T'

anomaly_clusters = [1]
error_threshold = 3

result_df = split_and_aggregate_by_cluster(df, time_interval, error_threshold, anomaly_clusters)

result_df.reset_index(inplace=True)

print(result_df)

result_df.to_csv('parsed-hdfs-data-aggregated.csv', index=False)
