import os
import time
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
from tqdm import tqdm  # For progress bars
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN') or 'place_your_github_token_here'
ORGANIZATION = 'place_you_organisation_name'
INPUT_CSV = 'githubrepofrom47.csv'  # Your new CSV file with repos having 500+ branches
OUTPUT_CSV = 'github_from3.csv'  # New CSV file with stale branch counts
CHECKPOINT_FILE = 'stale_branch_checkpoint_large.json'  # For saving progress

# Column containing repository names in your CSV file
REPO_COLUMN = 'repository_name'  # Based on your CSV

# Headers for GitHub API requests
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

# Rate limit configuration
API_CALL_DELAY = 0.2  # 200ms delay between API calls
BATCH_SIZE = 5  # Process fewer repos per batch since they're larger
BATCH_BREAK = 120  # Longer break between batches
CONNECTION_TIMEOUT = 45  # Increased timeout for API requests in seconds
MAX_RETRIES = 5  # Increased maximum number of retries for API calls

# Removed the MAX_BRANCHES_TO_CHECK limit to process all branches

def create_session():
    """Create a requests session with retry configuration."""
    session = requests.Session()
    retries = Retry(
        total=7,  # Increased retry limit
        backoff_factor=1.5,  # Increased backoff factor
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def check_rate_limit(session, wait_if_needed=True):
    """Check GitHub API rate limit status and wait if needed."""
    url = 'https://api.github.com/rate_limit'
    try:
        response = session.get(url, headers=headers, timeout=CONNECTION_TIMEOUT)

        if response.status_code != 200:
            print(f"Error checking rate limit: {response.status_code}")
            return False

        data = response.json()
        remaining = data['resources']['core']['remaining']
        reset_time = data['resources']['core']['reset']

        print(f"API calls remaining: {remaining}")

        if remaining < 200 and wait_if_needed:  # Increased threshold
            reset_timestamp = int(reset_time)
            current_timestamp = int(time.time())
            sleep_time = reset_timestamp - current_timestamp + 15  # Added 15 seconds buffer

            if sleep_time > 0:
                print(f"\nRate limit low ({remaining} remaining). Waiting {sleep_time} seconds until reset...")

                # Progress indicator while waiting
                for i in tqdm(range(sleep_time), desc="Waiting for rate limit reset"):
                    time.sleep(1)

                print("Continuing with API requests...")
                return True

        return True
    except Exception as e:
        print(f"Error in check_rate_limit: {str(e)}")
        return False

def safe_api_call(session, url, retry_count=0):
    """Make an API call with error handling and retries."""
    try:
        response = session.get(url, headers=headers, timeout=CONNECTION_TIMEOUT)
        time.sleep(API_CALL_DELAY)  # Rate limiting
        return response
    except (requests.exceptions.SSLError, requests.exceptions.ConnectionError,
            requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
        if retry_count < MAX_RETRIES:
            wait_time = 5 * (2 ** retry_count)  # Exponential backoff
            print(f"Connection error, retrying in {wait_time}s ({retry_count+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(wait_time)
            return safe_api_call(session, url, retry_count + 1)
        else:
            print(f"Failed after {MAX_RETRIES} retries: {str(e)}")
            return None

def load_checkpoint():
    """Load checkpoint data from file if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading checkpoint file: {str(e)}")
    return {}

def save_checkpoint(checkpoint_data):
    """Save checkpoint data to file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f)
        print(f"Checkpoint saved to {CHECKPOINT_FILE}")
    except Exception as e:
        print(f"Error saving checkpoint file: {str(e)}")

def get_stale_branch_count_rest(session, repo_full_name, checkpoint_data):
    """Get count of stale branches (>90 days old) using REST API with checkpointing."""
    if not check_rate_limit(session):
        return "Rate Limit Error"

    # Check if we have checkpoint data for this repo
    repo_checkpoint = checkpoint_data.get(repo_full_name, {})
    processed_branches = repo_checkpoint.get('processed_branches', [])
    stale_count = repo_checkpoint.get('stale_count', 0)

    # Get the default branch to exclude it from stale count
    repo_url = f'https://api.github.com/repos/{repo_full_name}'
    repo_response = safe_api_call(session, repo_url)

    if repo_response is None or repo_response.status_code != 200:
        print(f"Error fetching repo info for {repo_full_name}: {getattr(repo_response, 'status_code', 'N/A')}")
        default_branch = 'main'  # Fallback to a common default branch name
    else:
        repo_data = repo_response.json()
        default_branch = repo_data.get('default_branch', 'main')

    # Get all branches - we'll paginate through ALL branches
    all_branches = []
    page = 1
    pages_processed = repo_checkpoint.get('pages_processed', 0)

    if pages_processed > 0:
        print(f"Resuming from page {pages_processed + 1} for {repo_full_name}")
        page = pages_processed + 1
    else:
        print(f"Starting to fetch branches for {repo_full_name}")

    while True:
        branches_url = f'https://api.github.com/repos/{repo_full_name}/branches?per_page=100&page={page}'
        branches_response = safe_api_call(session, branches_url)

        if branches_response is None:
            return "Connection Error"
        elif branches_response.status_code == 404:
            print(f"Repository {repo_full_name} not found")
            return "Repo Not Found"
        elif branches_response.status_code != 200:
            print(f"Error fetching branches for {repo_full_name}: {branches_response.status_code}")
            return "Error"

        branches = branches_response.json()
        if not branches:
            break  # No more branches

        print(f"Fetched page {page} with {len(branches)} branches for {repo_full_name}")
        all_branches.extend(branches)

        # Update page counter in checkpoint
        repo_checkpoint['pages_processed'] = page
        checkpoint_data[repo_full_name] = repo_checkpoint
        save_checkpoint(checkpoint_data)

        page += 1

        # Check rate limit after each page of branches
        check_rate_limit(session)

    # Current time in seconds since epoch
    current_time = time.time()

    # 90 days in seconds
    stale_threshold = 90 * 24 * 60 * 60

    print(f"Fetched a total of {len(all_branches)} branches for {repo_full_name}")

    # Track current progress for checkpointing
    if 'total_branches' not in repo_checkpoint:
        repo_checkpoint['total_branches'] = len(all_branches)

    # Create a list of branches to process (excluding those already processed)
    branches_to_process = [branch for branch in all_branches if branch['name'] not in processed_branches]
    print(f"Processing {len(branches_to_process)} remaining branches (already processed {len(processed_branches)})")

    # Process branches in smaller chunks for large repositories
    chunk_size = min(50, len(branches_to_process))
    chunks_total = (len(branches_to_process) + chunk_size - 1) // chunk_size

    for chunk_idx in range(chunks_total):
        chunk_start = chunk_idx * chunk_size
        chunk_end = min(chunk_start + chunk_size, len(branches_to_process))
        chunk = branches_to_process[chunk_start:chunk_end]

        print(f"Processing chunk {chunk_idx + 1}/{chunks_total} ({chunk_end - chunk_start} branches)")

        # Use tqdm for a progress bar within this chunk
        for branch in tqdm(chunk, desc=f"Checking branches {chunk_start+1}-{chunk_end}/{len(branches_to_process)}"):
            # Skip default branch
            if branch['name'] == default_branch:
                processed_branches.append(branch['name'])
                continue

            # Get the latest commit on this branch
            commit_url = f'https://api.github.com/repos/{repo_full_name}/commits/{branch["commit"]["sha"]}'
            commit_response = safe_api_call(session, commit_url)

            if commit_response is None:
                continue  # Skip this branch if we couldn't get the commit info

            if commit_response.status_code != 200:
                print(f"Error fetching commit for branch {branch['name']}: {commit_response.status_code}")
                continue

            try:
                commit_data = commit_response.json()

                # Get commit date
                commit_date_str = commit_data['commit']['committer']['date']
                commit_date = datetime.strptime(commit_date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                commit_timestamp = commit_date.timestamp()

                # Check if older than 90 days
                if (current_time - commit_timestamp) > stale_threshold:
                    stale_count += 1
            except Exception as e:
                print(f"Error processing branch {branch['name']}: {str(e)}")

            # Mark this branch as processed
            processed_branches.append(branch['name'])

            # Update checkpoint after each branch
            repo_checkpoint['stale_count'] = stale_count
            repo_checkpoint['processed_branches'] = processed_branches
            checkpoint_data[repo_full_name] = repo_checkpoint
            save_checkpoint(checkpoint_data)

        # Take a break between chunks
        if chunk_idx < chunks_total - 1:
            print("Taking a short break between chunks...")
            time.sleep(5)
            check_rate_limit(session)

    print(f"Found {stale_count} stale branches in {repo_full_name}")
    return stale_count

def main():
    print(f"Starting stale branch analysis for large repositories in {ORGANIZATION}")
    print(f"Using input file: {INPUT_CSV}")
    print(f"Results will be saved to: {OUTPUT_CSV}")
    print(f"Using checkpoint file: {CHECKPOINT_FILE}")

    # Create a session with retry configuration
    session = create_session()

    # Load checkpoint data
    checkpoint_data = load_checkpoint()

    # Check rate limit before starting
    check_rate_limit(session, wait_if_needed=True)

    # Load existing CSV
    try:
        df = pd.read_csv(INPUT_CSV)
        print(f"Loaded {len(df)} repositories from {INPUT_CSV}")
    except Exception as e:
        print(f"Error loading CSV: {str(e)}")
        return

    # Verify the repo column exists
    if REPO_COLUMN not in df.columns:
        print(f"Error: Column '{REPO_COLUMN}' not found in CSV. Available columns are: {', '.join(df.columns)}")
        return

    # Add a new column for stale branches if it doesn't exist
    if 'Stale_Branches' not in df.columns:
        df['Stale_Branches'] = None

    # Find repositories that need processing
    repos_to_process = []
    for index, row in df.iterrows():
        repo_name = row[REPO_COLUMN]

        # Skip repos with 0 branches
        if pd.notna(row.get('number_of_branches')) and row['number_of_branches'] == 0:
            df.at[index, 'Stale_Branches'] = 0
            continue

        # Check if this repo is already completely processed
        repo_full_name = f"{ORGANIZATION}/{repo_name}"
        repo_checkpoint = checkpoint_data.get(repo_full_name, {})

        # If we have checkpoint data with processed branches equal to total branches, use the saved stale count
        if (repo_checkpoint.get('total_branches') and
                len(repo_checkpoint.get('processed_branches', [])) >= repo_checkpoint.get('total_branches')):
            print(f"Using cached result for {repo_name}: {repo_checkpoint.get('stale_count')} stale branches")
            df.at[index, 'Stale_Branches'] = repo_checkpoint.get('stale_count')
            continue

        # Otherwise, if not yet fully processed or not in checkpoint, add to processing list
        if (pd.isna(row.get('Stale_Branches')) or
                row.get('Stale_Branches') == "Error" or
                row.get('Stale_Branches') == ""):
            repos_to_process.append((index, repo_name))

    print(f"Need to process {len(repos_to_process)} repositories")

    # Process repositories in batches
    for batch_idx, batch in enumerate(range(0, len(repos_to_process), BATCH_SIZE)):
        batch_repos = repos_to_process[batch:batch + BATCH_SIZE]

        print(f"\n--- Processing Batch {batch_idx + 1} ({len(batch_repos)} repositories) ---")

        # Process each repository in the batch
        for i, (index, repo_name) in enumerate(batch_repos):
            repo_full_name = f"{ORGANIZATION}/{repo_name}"
            print(f"\nProcessing {i+1}/{len(batch_repos)} in batch: {repo_name}")
            print(f"Overall progress: {batch + i + 1}/{len(repos_to_process)}")

            # Get stale branch count
            stale_count = get_stale_branch_count_rest(session, repo_full_name, checkpoint_data)

            # Update the dataframe
            df.at[index, 'Stale_Branches'] = stale_count

            # Save progress after each repository
            df.to_csv(OUTPUT_CSV, index=False)
            print(f"Progress saved to {OUTPUT_CSV}")

            # Take a break between repos within a batch
            if i < len(batch_repos) - 1:
                print("Taking a short break between repositories...")
                time.sleep(10)
                check_rate_limit(session)

        # After each batch, take a break and check rate limits
        if batch + BATCH_SIZE < len(repos_to_process):
            print(f"\nCompleted batch {batch_idx + 1}. Taking a {BATCH_BREAK} second break...")

            # Save progress
            df.to_csv(OUTPUT_CSV, index=False)

            # Wait with progress bar
            for _ in tqdm(range(BATCH_BREAK), desc="Batch break"):
                time.sleep(1)

            # Check rate limit before starting next batch
            check_rate_limit(session, wait_if_needed=True)

    # Final save
    df.to_csv(OUTPUT_CSV, index=False)

    # Print summary
    print("\n=== Stale Branch Analysis Complete ===")
    print(f"Results saved to {OUTPUT_CSV}")

    # Calculate some stats
    try:
        # Filter out non-numeric values for calculations
        numeric_df = df[pd.to_numeric(df['Stale_Branches'], errors='coerce').notnull()]

        if len(numeric_df) > 0:
            total_stale = numeric_df['Stale_Branches'].sum()
            avg_stale = numeric_df['Stale_Branches'].mean()
            max_stale = numeric_df['Stale_Branches'].max()

            if not numeric_df['Stale_Branches'].empty:
                max_stale_repo = numeric_df.loc[numeric_df['Stale_Branches'].idxmax(), REPO_COLUMN]

                print(f"Total stale branches across all repositories: {total_stale}")
                print(f"Average stale branches per repository: {avg_stale:.2f}")
                print(f"Repository with most stale branches: {max_stale_repo} ({max_stale} stale branches)")

                # Count repositories with stale branches
                repos_with_stale = len(numeric_df[numeric_df['Stale_Branches'] > 0])
                print(f"Number of repositories with stale branches: {repos_with_stale} out of {len(numeric_df)} ({repos_with_stale/len(numeric_df)*100:.1f}%)")
        else:
            print("No numeric stale branch data available for statistics.")
    except Exception as e:
        print(f"Error calculating statistics: {str(e)}")

if __name__ == "__main__":
    main()