import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

INPUT_PATH = "data/processed_stock_data.csv"

def load_data(path=INPUT_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found. Run process_data.py first.")
    return pd.read_csv(path)

def prepare_features(df):
    # Keep numeric columns and drop rows with NaN
    feat_cols = [c for c in df.columns if c not in ('Date','ticker')]
    X = df[feat_cols].select_dtypes(include=['int64','float64']).fillna(0)
    return X

def train_and_report(df):
    # Use a simple binary target for demo: next-day return > 0
    df = df.sort_values('Date')
    df['Next_Return'] = df['Close'].pct_change().shift(-1)
    df['target'] = (df['Next_Return'] > 0).astype(int)
    df = df.dropna(subset=['target'])

    X = prepare_features(df)
    y = df['target']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)

    print("--- Model Training Report ---")
    print(f"Logistic Regression accuracy: {acc:.4f}")
    print("-----------------------------")

    # Save model probabilities alongside original dataframe for inspection
    df.loc[X_test.index, 'pred_proba'] = model.predict_proba(X_test)[:,1]
    out_path = os.path.join("output", "stock_model_predictions.csv")
    os.makedirs("output", exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Saved predictions to {out_path}")

def main():
    df = load_data()
    train_and_report(df)

if __name__ == "__main__":
    main()
