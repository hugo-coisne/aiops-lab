import pandas as pd

# Charger le fichier CSV
file_path = 'parsed-hdfs-data.csv'  # Remplacez par le chemin correct vers votre fichier
data = pd.read_csv(file_path)

# Vérifier que la colonne BlockId existe
if 'BlockId' in data.columns:
    # Obtenir le nombre de valeurs uniques dans BlockId
    unique_block_ids = data['BlockId'].nunique()
    print(f"Nombre de BlockId différents : {unique_block_ids}")
else:
    print("La colonne 'BlockId' n'existe pas dans le fichier.")
