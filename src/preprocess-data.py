import pandas as pd

# Charger les logs depuis le CSV contenant les données extraites
df = pd.read_csv('parsed-hdfs-data.csv')

# Combiner les colonnes 'Date' et 'Time' en une seule colonne datetime pour permettre un découpage basé sur le temps
# Ajouter errors='coerce' pour ignorer les lignes mal formées et les convertir en NaT
df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str), format='%y%m%d %H%M%S', errors='coerce')

# Vérification des lignes où la conversion a échoué (NaT)
print("Lignes avec des dates mal formées :")
print(df[df['Datetime'].isna()])

# Trier le DataFrame par la colonne 'Datetime' pour être sûr qu'il soit dans l'ordre chronologique
df = df.sort_values(by='Datetime')

# Fonction pour diviser le DataFrame en fonction des intervalles de temps et agréger par le nombre de Cluster ID
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
    # Définir 'Datetime' comme index pour le découpage basé sur le temps
    df.set_index('Datetime', inplace=True)

    # Redécouper en fonction des intervalles de temps et compter les occurrences de chaque Cluster ID
    cluster_counts = df.groupby([pd.Grouper(freq=time_interval), 'Cluster ID']).size().unstack(fill_value=0)

    # Initialiser une colonne vide pour l'étiquette d'anomalie
    cluster_counts['Anomaly'] = '0'

    # Marquer le morceau comme 'anomalie' en fonction du seuil spécifié et des clusters spécifiques
    for index, row in cluster_counts.iterrows():
        if anomaly_clusters:
            # Vérifier si l'un des clusters anormaux dépasse le seuil d'erreur
            if row[anomaly_clusters].sum() >= error_threshold:
                cluster_counts.at[index, 'Anomaly'] = '1'
        else:
            # Vérifier si un cluster dépasse le seuil
            if row.sum() >= error_threshold:
                cluster_counts.at[index, 'Anomaly'] = '1'

    # Supprimer les colonnes correspondant aux Cluster ID dans anomaly_clusters
    if anomaly_clusters:
        cluster_counts.drop(columns=anomaly_clusters, inplace=True)

    return cluster_counts

# Définir l'intervalle de temps pour découper les logs (par exemple, '30T' pour 30 minutes)
time_interval = '30T'  # Modifier selon l'intervalle de temps souhaité

# Définir les conditions d'anomalie (clusters spécifiques ou seuils)
anomaly_clusters = [1]  # Définir les Cluster IDs considérés comme anormaux
error_threshold = 3  # Si la somme des occurrences de ces clusters dans un intervalle dépasse ce seuil, étiqueter comme anomalie

# Diviser et agréger le DataFrame en morceaux en fonction de l'intervalle de temps spécifié
result_df = split_and_aggregate_by_cluster(df, time_interval, error_threshold, anomaly_clusters)

# Réinitialiser l'index pour aplatir le DataFrame, avec les intervalles de temps comme lignes
result_df.reset_index(inplace=True)

# Afficher le DataFrame résultant
print(result_df)

# Optionnel : enregistrer le résultat dans un nouveau fichier CSV
result_df.to_csv('parsed-hdfs-data-aggregated.csv', index=False)
