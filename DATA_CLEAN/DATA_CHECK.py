import os
import pandas as pd

# === Configuration ===
data_dir = './Data_Clean_Filled'

# === Statistics Collector ===
result = []

for file in os.listdir(data_dir):
    if file.endswith('.csv'):
        file_path = os.path.join(data_dir, file)
        df = pd.read_csv(file_path)

        total_rows = df.shape[0]
        missing_values = df.iloc[:, 3].isna().sum()

        result.append({
            'File': file,
            'TotalRows': total_rows,
            'MissingIn4thColumn': missing_values
        })

# === Output Result ===
result_df = pd.DataFrame(result)
print(result_df)

result_df.to_csv('Missing_Summary.csv', index=False)
print("✔ Summary Saved To Missing_Summary.csv")
