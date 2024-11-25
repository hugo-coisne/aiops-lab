import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

# Charger les données
df = pd.read_csv('HDFS_v1-aggregated.csv')

# Assurer que les valeurs des événements (E1 à Ex) sont numériques et les NaN remplacés par 0 (si besoin)
event_columns = [col for col in df.columns if col.startswith('E')]  # E1, E2, ..., Ex

# Utilisation de LabelEncoder pour la colonne 'BlockId' (unique)
label_encoder = LabelEncoder()
df['BlockId'] = label_encoder.fit_transform(df['BlockId'])

# Sélectionner les caractéristiques et l'étiquette (Anomaly)
X = df[event_columns]
y = df['Anomaly'].astype(int)

# Séparer les données en jeu d'entraînement (60%), de validation (20%), et de test (20%)
train_size = int(0.60 * len(df))
val_size = int(0.20 * len(df))
test_size = len(df) - train_size - val_size

train_df = df.iloc[:train_size]
val_df = df.iloc[train_size:train_size + val_size]
test_df = df.iloc[train_size + val_size:]

# Séparer les caractéristiques (features) et les étiquettes (labels)
X_train = train_df[event_columns]
y_train = train_df['Anomaly'].astype(int)

X_val = val_df[event_columns]
y_val = val_df['Anomaly'].astype(int)

X_test = test_df[event_columns]
y_test = test_df['Anomaly'].astype(int)

# Entraîner les modèles
rf_model = RandomForestClassifier()
rf_model.fit(X_train, y_train)
rf_pred = rf_model.predict(X_test)

lr_model = LogisticRegression(max_iter=1000)
lr_model.fit(X_train, y_train)
lr_pred = lr_model.predict(X_test)

# Fonction pour calculer les métriques de performance
def get_metrics(y_true, y_pred, model_name):
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    auc_score = roc_auc_score(y_true, y_pred)
    
    print(f'{model_name} - Accuracy: {accuracy:.3f}, Precision: {precision:.3f}, Recall: {recall:.3f}, AUC: {auc_score:.3f}')
    
    return [accuracy, precision, recall, auc_score]

# Calculer les métriques pour chaque modèle et afficher dans la console
rf_metrics = get_metrics(y_test, rf_pred, "Random Forest")
lr_metrics = get_metrics(y_test, lr_pred, "Régression Logistique")

# Organiser les résultats dans un DataFrame
metrics_df = pd.DataFrame([rf_metrics, lr_metrics], 
                           columns=['Accuracy', 'Precision', 'Recall', 'AUC'],
                           index=['Random Forest', 'Régression Logistique'])

# Afficher un graphique pour les métriques de RandomForest
metrics_df.loc['Random Forest'].plot(kind='bar', figsize=(8, 6), color=['#007BFF', '#28A745', '#FFC107', '#DC3545'])
plt.title('Random Forest - Performance Metrics')
plt.ylabel('Score')
plt.xlabel('Metric')
plt.xticks(rotation=0)
plt.show()

# Afficher un graphique pour les métriques de Logistic Regression
metrics_df.loc['Régression Logistique'].plot(kind='bar', figsize=(8, 6), color=['#007BFF', '#28A745', '#FFC107', '#DC3545'])
plt.title('Logistic Regression - Performance Metrics')
plt.ylabel('Score')
plt.xlabel('Metric')
plt.xticks(rotation=0)
plt.show()


# Matrice de corrélation des événements
corr_matrix = df[event_columns].corr()

# Masquer les corrélations faibles (en dessous de 0.5, par exemple)
corr_matrix_filtered = corr_matrix.where(corr_matrix.abs() > 0.5)

# Affichage de la matrice de corrélation avec des ajustements visuels
plt.figure(figsize=(12, 10))
sns.heatmap(corr_matrix_filtered, annot=True, cmap='coolwarm', fmt='.1f', linewidths=0.5,
            cbar_kws={'shrink': 0.8}, square=True, vmin=-1, vmax=1)

plt.title('Matrice de Corrélation des Événements (corrélations > 0.5)', fontsize=16)
plt.show()
