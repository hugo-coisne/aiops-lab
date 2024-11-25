import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score

# Load the preprocessed
df = pd.read_csv('csv/BGL-aggregated2.csv')

# Sort the data by Datetime to ensure it's time-ordered
df = df.sort_values(by='Window Start')

# 60 / 20 / 20
train_proportion = .6
val_proportion = .2
train_size = int(train_proportion * len(df))
val_size = int(val_proportion * len(df))
test_size = len(df) - train_size - val_size

train_df = df.iloc[:train_size]
val_df = df.iloc[train_size:train_size + val_size]
test_df = df.iloc[train_size + val_size:]

# Separate features and labels
X_train = train_df.drop(columns=['Window Start', 'Window End', 'Anomaly'])  # Features
y_train = train_df['Anomaly'].astype(int)  # Labels

X_val = val_df.drop(columns=['Window Start', 'Window End', 'Anomaly'])
y_val = val_df['Anomaly'].astype(int)

X_test = test_df.drop(columns=['Window Start', 'Window End', 'Anomaly'])
y_test = test_df['Anomaly'].astype(int)

# Train
rf_model = RandomForestClassifier()
rf_model.fit(X_train, y_train)
rf_pred = rf_model.predict(X_test)

lr_model = LogisticRegression(max_iter=1000)
lr_model.fit(X_train, y_train)
lr_pred = lr_model.predict(X_test)

# Evaluation function
def evaluate_model(y_true, y_pred, model_name):
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    auc = roc_auc_score(y_true, y_pred)
    
    print(f"Performance of {model_name}:")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"AUC: {auc:.4f}")
    print('-' * 30)

# Evaluate each model on the test set
evaluate_model(y_test, rf_pred, "Random Forest")
evaluate_model(y_test, lr_pred, "Logistic Regression")
