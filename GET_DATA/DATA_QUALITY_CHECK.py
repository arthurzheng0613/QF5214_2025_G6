import os
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

# === Set the working directory ===
data_dir = './Data'  # Change to absolute path if needed, e.g., 'D:/project/Data'

# Containers for results and files to be deleted
results = []
files_to_delete = []

# === Iterate over all csv files in the Data folder ===
csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv') and f.startswith('US_')]

for filename in tqdm(csv_files, desc="Processing CSV files"):
    file_path = os.path.join(data_dir, filename)

    # Read the csv file
    df = pd.read_csv(file_path)
    if df.shape[1] < 4:
        print(f"Warning: {filename} has less than 4 columns, skipped.")
        continue

    # Extract the 4th column (index 3) as data column
    data_col = df.iloc[:, 3]

    # Count total, -999, 0, and NaN occurrences
    total_count = data_col.shape[0]
    count_neg999 = (data_col == -999).sum()
    count_zero = (data_col == 0).sum()
    count_nan = data_col.isna().sum()
    bad_count = count_neg999 + count_zero + count_nan
    bad_ratio = bad_count / total_count if total_count > 0 else 0

    # Record result for this file
    results.append({
        'file': filename,
        'total_count': total_count,
        'count_-999': count_neg999,
        'count_0': count_zero,
        'count_nan': count_nan,
        'bad_ratio': bad_ratio
    })

    # Delete the file if bad_ratio >= 25%
    if bad_ratio >= 0.25:
        files_to_delete.append(filename)
        os.remove(file_path)

# === Save the result summary ===
results_df = pd.DataFrame(results)
results_df.to_csv('Data_Quality_Summary.csv', index=False)

print("\nFiles deleted due to bad_ratio >= 25%:")
print(files_to_delete)
print("\nData quality summary saved as data_quality_summary.csv")

# === Plot bad_ratio distribution ===
plt.figure(figsize=(10, 6))
plt.hist(results_df['bad_ratio'], bins=20, edgecolor='black')
plt.title("Bad Value Ratio Distribution")
plt.xlabel("Bad Ratio")
plt.ylabel("Number of Files")
plt.grid(True)
plt.tight_layout()
plt.savefig("Bad_Ratio_Distribution.png")
plt.show()

print("Bad Ratio Distribution Plot saved as Bad_Ratio_Distribution.png")