"""
Train baseline LULC models using Google Satellite Embedding V1 (2024).
Handles extreme class imbalance with SMOTE.
Uses Spatial K-Fold Cross-Validation to evaluate true performance
without spatial autocorrelation (data leakage).
"""

import os
import pandas as pd
import numpy as np
from collections import Counter

from imblearn.over_sampling import SMOTE
from sklearn.model_selection import GroupKFold
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, cohen_kappa_score, f1_score, accuracy_score, balanced_accuracy_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns

# ── Configuration ──────────────────────────────────────────────────────────
BASE = r"c:\Users\Kdixter\Desktop\GIS_Analysis_Final"
INPUT_DATA = os.path.join(BASE, "raw_data", "dataset_1", "dataset_1_embeddings.csv")

# Ensure models dir exists
os.makedirs(os.path.join(BASE, "models"), exist_ok=True)
OUTPUT_REPORT = os.path.join(BASE, "models", "spatial_baseline_embedding_report.txt")


def load_and_prep_data():
    print(f"Loading data from {INPUT_DATA}...")
    df = pd.read_csv(INPUT_DATA)
    
    # Features (A00 - A63)
    emb_cols = [f"A{i:02d}" for i in range(64)]
    
    # Drop rows with null embeddings (safety check)
    df = df.dropna(subset=emb_cols)
    print(f"Total valid points: {len(df):,}")
    
    X = df[emb_cols].values
    y_raw = df["bin"].values
    coords = df[['lat', 'lon']].values
    
    print("\nClass Distribution (Original):")
    counts = Counter(y_raw)
    for k, v in counts.items():
        print(f"  {k}: {v:,} ({v/len(df)*100:.1f}%)")
        
    # Encode labels
    le = LabelEncoder()
    y = le.fit_transform(y_raw)
    
    # ── Create Spatial Blocks (Clusters) for GroupKFold ───────────────────────
    # We cluster the lat/lon coordinates into 5 geographic regions.
    # GroupKFold ensures that all points from a specific region stay together
    # in either the train OR test set, preventing spatial leakage.
    print("\nClustering coordinates into 5 geographic blocks for Spatial K-Fold...")
    n_blocks = 5
    kmeans = KMeans(n_clusters=n_blocks, random_state=42, n_init='auto')
    spatial_groups = kmeans.fit_predict(coords)
    
    for i in range(n_blocks):
        print(f"  Block {i}: {np.sum(spatial_groups == i):,} points")
    
    return X, y, coords, spatial_groups, le.classes_


def main():
    X, y, coords, groups, class_names = load_and_prep_data()
    n_splits = 5
    gkf = GroupKFold(n_splits=n_splits)

    # ── Initialize Models ─────────────────────────────────────────────────────
    models = {
        "RandomForest": RandomForestClassifier(
            n_estimators=100,
            max_depth=15,
            n_jobs=-1,
            random_state=42,
            class_weight="balanced"
        ),
        "XGBoost": XGBClassifier(
            n_estimators=100,
            max_depth=8,
            n_jobs=-1,
            random_state=42,
            eval_metric='mlogloss',
            tree_method='hist'
        )
    }

    results = {name: [] for name in models.keys()}
    cm_accumulated = {name: np.zeros((len(class_names), len(class_names)), dtype=int) for name in models.keys()}

    # ── Spatial K-Fold Evaluation ─────────────────────────────────────────────
    print("\nStarting 5-Fold Spatial Cross-Validation...")
    
    for fold, (train_idx, test_idx) in enumerate(gkf.split(X, y, groups), 1):
        print(f"\n--- Fold {fold} ---")
        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]
        
        print(f"  Train: {len(X_train):,} points | Test: {len(X_test):,} points")
        
        # Apply SMOTE *only* to the training fold to prevent data leakage into test
        smote = SMOTE(random_state=42)
        X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

        for name, model in models.items():
            print(f"  Training {name}...", end=" ", flush=True)
            model.fit(X_train_smote, y_train_smote)
            
            y_pred = model.predict(X_test)
            
            macro_f1 = f1_score(y_test, y_pred, average='macro')
            results[name].append({
                "acc": accuracy_score(y_test, y_pred),
                "bal_acc": balanced_accuracy_score(y_test, y_pred),
                "macro_f1": macro_f1,
                "kappa": cohen_kappa_score(y_test, y_pred)
            })
            cm_accumulated[name] += confusion_matrix(y_test, y_pred, labels=range(len(class_names)))
            print(f"done (Macro F1 = {macro_f1:.4f})")

    # ── Final Reporting ───────────────────────────────────────────────────────
    with open(OUTPUT_REPORT, "w") as f:
        f.write("="*60 + "\n")
        f.write("SPATIAL K-FOLD EVALUATION: SATELLITE EMBEDDINGS (2024)\n")
        f.write("="*60 + "\n\n")

        print("\n" + "="*40)
        print("AVERAGE SPATIAL PERFORMANCE ACROSS 5 FOLDS:")
        print("="*40)

        for name in models.keys():
            df_res = pd.DataFrame(results[name])
            mean_res = df_res.mean()
            std_res = df_res.std()
            
            summary_str = (
                f"{name}:\n"
                f"  Accuracy:  {mean_res['acc']:.4f} (±{std_res['acc']:.4f})\n"
                f"  Bal_Acc:   {mean_res['bal_acc']:.4f} (±{std_res['bal_acc']:.4f})\n"
                f"  Macro F1:  {mean_res['macro_f1']:.4f} (±{std_res['macro_f1']:.4f})\n"
                f"  Kappa:     {mean_res['kappa']:.4f} (±{std_res['kappa']:.4f})\n"
            )
            print(summary_str)
            
            f.write(f"--- Model: {name} ---\n")
            f.write(summary_str + "\n")
            
            # Plot Accumulated Confusion Matrix
            plt.figure(figsize=(10, 8))
            sns.heatmap(cm_accumulated[name], annot=True, fmt='d', cmap='Blues', 
                        xticklabels=class_names, yticklabels=class_names)
            plt.title(f"{name} Spatial CV Accumulated Confusion Matrix")
            plt.ylabel('True Label')
            plt.xlabel('Predicted Label')
            cm_path = os.path.join(BASE, "models", f"{name}_spatial_confusion_matrix.png")
            plt.savefig(cm_path, bbox_inches='tight')
            plt.close()
            print(f"  Saved spatial confusion matrix to {cm_path}")

    print(f"\nEvaluation complete. Full report saved to: \n  {OUTPUT_REPORT}")


if __name__ == "__main__":
    main()

