import pandas as pd
import os
from tqdm import tqdm
import matplotlib.pyplot as plt

# === Configuration ===

# Data Folder
data_dir = './Data_Clean_Filled'

# Selected Variables

variable_list = ['GWETTOP', 'T10M_MIN', 'V10M', 'TS_MAX', 'V50M', 'V2M', 'PBLTOP', 'WS50M',
                 'WS50M_RANGE', 'T10M', 'U2M', 'WS2M_MIN', 'TO3', 'TROPT', 'TS', 'T2M',
                 'T2M_RANGE', 'QV2M', 'WD2M', 'QV10M', 'T2MDEW', 'WS10M', 'RHOA', 'TS_RANGE',
                 'U10M', 'WD50M', 'TROPPB', 'WS2M_RANGE', 'RH2M', 'T2MWET', 'U50M', 'WS10M_MAX',
                 'T10M_RANGE', 'TS_MIN', 'WS2M_MAX', 'PW', 'TQV', 'PSH', 'T10M_MAX', 'T2M_MAX',
                 'TROPQ', 'WS2M', 'WS50M_MIN', 'T2M_MIN', 'WD10M', 'SLP', 'TS_ADJ']

'''
variable_list = ['ALLSKY_SFC_PAR_DIFF', 'CLRSKY_KT', 'ORIGINAL_ALLSKY_SFC_SW_DIFF', 'MIDDAY_INSOL', 
                 'AOD_55', 'ALLSKY_SFC_SW_DWN', 'CLRSKY_SFC_PAR_DIRH', 'CLOUD_AMT_NIGHT', 
                 'CLRSKY_SFC_SW_UP', 'CLRSKY_NKT', 'CLRSKY_SFC_LW_UP', 'ALLSKY_SFC_SW_UP', 
                 'CLRSKY_SFC_SW_DIFF', 'ALLSKY_SFC_SW_DIFF', 'TOA_SW_DWN', 'ALLSKY_SFC_SW_DIRH', 
                 'CLRSKY_SRF_ALB', 'ALLSKY_SRF_ALB', 'CLOUD_AMT', 'AOD_84', 'SRF_ALB_ADJ', 
                 'ALLSKY_SFC_PAR_DIRH', 'ALLSKY_SFC_SW_DNI', 'ALLSKY_SFC_PAR_TOT', 'ALLSKY_SFC_UVA', 
                 'ALLSKY_SFC_LW_DWN', 'ALLSKY_KT', 'ORIGINAL_ALLSKY_SFC_SW_DIRH', 'ALLSKY_SFC_LW_UP', 
                 'CLRSKY_SFC_PAR_TOT', 'ALLSKY_SFC_UV_INDEX', 'AIRMASS', 'CLRSKY_SFC_SW_DIRH', 
                 'CLRSKY_SFC_SW_DNI', 'TOA_SW_DNI', 'CLOUD_OD', 'CLOUD_AMT_DAY', 'CLRSKY_SFC_LW_DWN',
                 'ALLSKY_NKT', 'CDD0', 'CLRSKY_SFC_PAR_DIFF', 'CLRSKY_SFC_SW_DWN', 'AOD_55_ADJ']
'''

# Output File

output_file = 'US_Meteorology_20180101_20241231_Daily.csv'
'''
output_file = 'US_Radiation_20180101_20241231_Daily.csv'
'''

# === Merge Process ===

merged_df = None
missing_info = {}

print("Starting merging process...")

for var in tqdm(variable_list, desc="Merging Variables"):
    file_name = f'US_{var}_20180101_20241231_Daily.csv'
    file_path = os.path.join(data_dir, file_name)

    if not os.path.exists(file_path):
        print(f"Warning: {file_name} not found, skipped.")
        continue

    # Read CSV
    df = pd.read_csv(file_path)

    # Rename Variable Column
    df.rename(columns={df.columns[3]: var}, inplace=True)

    # Keep Necessary Columns
    df = df.iloc[:, [0, 1, 2, 3]]

    # Merge
    if merged_df is None:
        merged_df = df
    else:
        merged_df = pd.merge(merged_df, df, on=['Longitude', 'Latitude', 'Timestamp'], how='outer')

print("\nMerging Completed.")

# === Missing Value Check ===

print("\nChecking Missing Ratios...")

# Compute Missing Ratio for Each Variable
for var in variable_list:
    if var in merged_df.columns:
        total = merged_df.shape[0]
        missing = merged_df[var].isna().sum()
        missing_ratio = missing / total
        missing_info[var] = missing_ratio

# Display Missing Info
print("\nMissing Ratio for Each Variable:")
for var, ratio in missing_info.items():
    print(f"{var}: {ratio:.2%}")

# === Save Merged Data ===
merged_df = merged_df.sort_values(by=['Longitude', 'Latitude', 'Timestamp']).reset_index(drop=True)
merged_df.to_csv(output_file, index=False)
print(f"\nMerged CSV saved as: {output_file}")

# === Plot Missing Ratio ===

plt.figure(figsize=(max(10, len(missing_info) * 0.4), 6))
plt.bar(missing_info.keys(), [v * 100 for v in missing_info.values()])
plt.ylabel("Missing Ratio (%)")
plt.xlabel("Variables")
plt.title("Missing Ratio per Variable of Meteorology")
plt.xticks(rotation=45, ha='right')
plt.grid(True, axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("Meteorology_Missing_Ratio_Barplot.png")
plt.show()
print("Meteorology Missing Ratio Barplot saved as: Meteorology_Missing_Ratio_Barplot.png")

'''
plt.figure(figsize=(max(10, len(missing_info) * 0.4), 6))
plt.bar(missing_info.keys(), [v * 100 for v in missing_info.values()])
plt.ylabel("Missing Ratio (%)")
plt.xlabel("Variables")
plt.title("Missing Ratio per Variable of Radiation")
plt.xticks(rotation=45, ha='right')
plt.grid(True, axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("Radiation_Missing_Ratio_Barplot.png")
plt.show()
print("Radiation Missing Ratio Barplot saved as: Radiation_Missing_Ratio_Barplot.png")
'''