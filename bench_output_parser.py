import os
import re
import csv

# Folder where your .txt files are
input_folder = '/Users/dorcaswong/Desktop/Concurrency/YCSB/dorcas/'  # <-- change this!

# What we want to extract
categories_of_interest = ['READ-MODIFY-WRITE', 'INSERT']
metrics_of_interest = ['AverageLatency(us)', 'MinLatency(us)', 'MaxLatency(us)']

# Holds extracted data
data = []

# Helper function to parse a single file
def parse_file(filepath):
    results = {'filename': os.path.relpath(filepath, input_folder)}
    with open(filepath, 'r') as f:
        content = f.readlines()
    for line in content:
        line = line.strip()
        match = re.match(r'\[(\w+)\],\s*(.+?),\s*(.+)', line)
        if match:
            category, metric, value = match.groups()
            if category in categories_of_interest and metric in metrics_of_interest:
                key = f'{category}_{metric}'
                results[key] = value
    return results

# Walk through all subdirectories and files
for root, dirs, files in os.walk(input_folder):
    for filename in files:
        if filename.endswith('.txt'):
            filepath = os.path.join(root, filename)
            file_data = parse_file(filepath)
            data.append(file_data)

# Columns for output
columns = ['filename']
for cat in categories_of_interest:
    for metric in metrics_of_interest:
        columns.append(f'{cat}_{metric}')

# Print the results nicely
print('\t'.join(columns))
for entry in data:
    print('\t'.join(entry.get(col, 'N/A') for col in columns))

# (Optional) Save to CSV
output_csv = os.path.join(input_folder, 'insert_rmw_summary.csv')
with open(output_csv, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=columns)
    writer.writeheader()
    for entry in data:
        writer.writerow(entry)

print(f"\nSummary saved to {output_csv}")
