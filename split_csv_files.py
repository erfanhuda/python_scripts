import pandas as pd
import argparse

parser = argparse.ArgumentParser()
# Function to split CSV into parts based on N rows
def split_csv(input_file, output_prefix, rows_per_part):
    # Read the entire CSV file into a pandas DataFrame
    df = pd.read_csv(input_file)
    
    # Split the DataFrame into chunks
    chunks = [df[i:i+rows_per_part] for i in range(0, len(df), rows_per_part)]
    
    # Save each chunk as a separate CSV file
    for i, chunk in enumerate(chunks):
        output_file = f"{output_prefix}_{i}.csv"
        chunk.to_csv(output_file, index=False)
        print(f"Part {i} saved to {output_file} with {len(chunk)} rows.")

# Argparse
parser.add_argument("--file", type=str, required=True)
parser.add_argument("--prefix", type=str, required=False, default="output_prefix")
parser.add_argument("--size", type=str, required=False, default=100000)
args = parser.parse_args()

input_file = f"{args.file}.csv"
prefix = f"{args.prefix}"
size = int(args.size)

# Call the function with your CSV file, output file prefix, and number of rows per part (N)
split_csv(input_file,prefix, size)