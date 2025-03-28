import json
import pandas as pd
import numpy as np

# Load JSON file from current directory
file_path = "stale_branch_detailed_checkpoint.json"
with open(file_path, "r") as file:
    data = json.load(file)

# Initialize an empty list to collect all repository data
all_repos_data = []

# Keep track of repositories to add empty rows between them
first_repo = True

# Process data for each repository
for repo_name, repo_data in data.items():
    # Extract stale branches information
    stale_branches = repo_data.get("stale_branches_info", [])

    # If not the first repository, add an empty row to separate repositories
    if not first_repo:
        all_repos_data.append([np.nan] * 4)  # 4 columns

    # Create rows for this repository
    for branch in stale_branches:
        row = [
            repo_name,
            branch["branch_name"],
            branch["last_commit_date"],
            branch["last_merged_to"]
        ]
        all_repos_data.append(row)

    # Ensure we don't treat the first repository as special after first iteration
    first_repo = False

# Create DataFrame
df = pd.DataFrame(all_repos_data, columns=["Repository Name", "Branch Name", "Last Commit Date", "Last Merged To"])

# Save to single CSV
csv_output_path = "reposcsv1.csv"
df.to_csv(csv_output_path, index=False)
print(f"Created combined CSV: {csv_output_path}")