import logging
import pandas as pd
import time
import argparse
import os
import glob

start_time = time.time()
parser = argparse.ArgumentParser()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def convert_csv_to_excel(csv_file, excel_file):
    # Read the CSV file
    df = pd.read_csv(csv_file, dtype=str)

    # Write the DataFrame to an Excel file
    df.to_excel(excel_file, index=False, header=True)


# Specify the paths for the CSV and Excel files
parser.add_argument("--file", type=str, required=False, default=None)
parser.add_argument("--dir", type=str, required=False, default=None)
args = parser.parse_args()

dir_path = []
excel_file_paths = []

if args.file is None and args.dir is None:
    logging.error("No file specified. CSV to Excel converter stopped.")

elif args.file is not None and args.dir is None:
    logging.info("Converting file %s" % f"{args.file}")

    # Convert CSV to Excel
    if args.file.endswith(".csv"):
        csv_file_path = args.file
        excel_file_path = ".".join([args.file.split(".csv")[0], "xlsx"])
    else:
        csv_file_path = f'{args.file}.csv'
        excel_file_path = f'{args.file}.xlsx'

    convert_csv_to_excel(csv_file_path, excel_file_path)

    end_time = time.time()
    logging.info(f"Execution duration {end_time - start_time} seconds")

elif args.file is None and args.dir is not None:
    logging.info("Converting directory %s" % f"{args.dir}")
    # Convert multiple CSV
    dir_path = glob.glob(os.path.join(args.dir, "*.csv"))
    excel_file_paths = [".".join([file.split(".csv")[0], "xlsx"])
                        for file in dir_path if file.endswith(".csv")]

    for item in dir_path:
        for output in excel_file_paths:
            convert_csv_to_excel(item, output)

    end_time = time.time()
    logging.info(f"Execution duration {end_time - start_time} seconds")

elif args.file is not None and args.dir is not None:
    logging.info("Converting file {} and directory {}".format(
        f"{args.file}", f"{args.dir}"))
    # Convert CSV to Excel
    if args.file.endswith(".csv"):
        csv_file_path = args.file
        excel_file_path = ".".join([args.file.split(".csv")[0], "xlsx"])
    else:
        csv_file_path = f'{args.file}.csv'
        excel_file_path = f'{args.file}.xlsx'
    # Convert multiple CSV
    dir_path = glob.glob(os.path.join(args.dir, "*.csv"))
    excel_file_paths = [".".join([file.split(".csv")[0], "xlsx"])
                        for file in dir_path if file.endswith(".csv")]

    for item in dir_path:
        for output in excel_file_paths:
            convert_csv_to_excel(item, output)

    convert_csv_to_excel(csv_file_path, excel_file_path)

    end_time = time.time()
    logging.info(f"Execution duration {end_time - start_time} seconds")

else:
    raise ValueError("Could not run operation")
