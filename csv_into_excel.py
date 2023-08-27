import pandas as pd
import argparse 
from pathlib import Path
import win32com.client  as win32

parser = argparse.ArgumentParser()

def convert_csv_to_excel(csv_file, excel_file):
    # Read the CSV file
    df = pd.read_csv(csv_file, dtype=str)

    # Write the DataFrame to an Excel file
    df.to_excel(excel_file, index=False, header=True)

# Specify the paths for the CSV and Excel files
parser.add_argument("--file", type=str, required=True)
args = parser.parse_args()

csv_file_path = f'{args.file}.csv'
excel_file_path = f'{args.file}.xlsx'

# Convert CSV to Excel
convert_csv_to_excel(csv_file_path, excel_file_path)

out_file = Path.cwd() / str(excel_file_path)

# Open up Excel and make it visible
excel = win32.gencache.EnsureDispatch('Excel.Application')
excel.Visible = True

# Open up the file
excel.Workbooks.Open(out_file)

# Wait before closing it
_ = input("Press enter to close Excel")
excel.Application.Quit()