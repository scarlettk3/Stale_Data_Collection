import re
import pandas as pd
import os
from pathlib import Path

def parse_repo_output(file_path):
    """Parse the output from the terminal to extract repository names and branch counts"""
    with open(file_path, 'r') as file:
        content = file.read()

    # Regular expression to match the repository name and branch count
    pattern = r"Processed \d+/\d+: ([\w\-]+) - (\d+) branches"
    matches = re.findall(pattern, content)

    # Convert matches to a list of dictionaries
    results = []
    for repo, branches in matches:
        results.append({
            'repository_name': repo,
            'number_of_branches': int(branches)
        })

    return results

def save_to_csv(data, output_path):
    """Save the data to a CSV file"""
    try:
        # Create DataFrame from the data
        df = pd.DataFrame(data)

        # No sorting - keep original order

        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save to CSV
        df.to_csv(output_path, index=False)
        print(f"Data successfully saved to {output_path}")
        return True
    except PermissionError:
        print(f"Permission denied while trying to write to {output_path}")
        print("Please try a different location or close any programs that might be using this file.")
        return False
    except Exception as e:
        print(f"Error saving data: {str(e)}")
        return False

def main():
    # Path to the text file containing terminal output
    input_file = input("Enter the path to the text file containing terminal output: ")

    # Parse the terminal output
    results = parse_repo_output(input_file)

    # Print total repositories found
    print(f"Found {len(results)} repositories")

    # Create a user-friendly path in the Downloads folder
    user_home = Path.home()
    downloads_folder = user_home / "Downloads"
    default_output_path = downloads_folder / "github_rep_analysis.csv"

    # Ask for output path with default
    output_path = input(f"Enter the path to save the CSV file [default: {default_output_path}]: ")
    if not output_path:
        output_path = default_output_path

    # Try to save the CSV file
    success = False
    while not success:
        success = save_to_csv(results, output_path)
        if not success:
            output_path = input("Enter a new path to save the CSV file (or 'q' to quit): ")
            if output_path.lower() == 'q':
                print("Operation cancelled by user.")
                break

if __name__ == "__main__":
    main()