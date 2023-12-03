import pandas as pd
import glob
import os

DIR = "C:/Users/muhammad.huda/Downloads/LGD"
csv_files = glob.glob(os.path.join(DIR, "*.csv"))
df = []

for item in csv_files:
    df_temp = pd.read_csv(
        item, parse_dates=['a.mom_date', 'a.pt_date', 'a.first_default_date'])
    df.append(df_temp)

# df1 = pd.read_csv(FILE)
new_df = pd.concat(df)
new_df['a.no'].sort_values(ascending=True)
new_df[['a.loan_no', 'a.pd_segment', 'a.lgd_segment']].to_string()
new_df.info()

df_summary = pd.DataFrame()
df_summary['recovery_amount'] = new_df['a.cur_balance'].sum()
df_summary['recovery_amount'] = new_df['a.cur_balance_mom'].sum()
df_summary['first_default_principal'] = new_df['a.first_default_principal'].sum()
df_summary['recovery_amount'] = new_df['a.recovery_amount'].sum()
df_summary['pv_recovery_amount'] = new_df['a.pv_recovery_amount'].sum()

df_summary.to_csv("./summary.xlsx")
