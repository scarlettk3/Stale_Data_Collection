import os
import time
import pandas as pd
from tabulate import tabulate
import requests

# Configuration
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN') or 'place_you_github_token_here'
ORGANIZATION = ' ' # Your GitHub organization name
TEAM_NAME = ' ' # Your team name within the organization

# Headers for GitHub API
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_team_id():
    """Get the numeric team ID from the team name."""
    url = f'https://api.github.com/orgs/{ORGANIZATION}/teams'
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error fetching teams: {response.status_code}")
        print(response.json())
        return None

    teams = response.json()
    for team in teams:
        if team['name'].lower() == TEAM_NAME.lower() or team['slug'].lower() == TEAM_NAME.lower():
            return team['id']

    # If team not found in first page, try to search more pages
    page = 2
    while True:
        url = f'https://api.github.com/orgs/{ORGANIZATION}/teams?per_page=100&page={page}'
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            break

        teams = response.json()
        if not teams: # No more teams
            break

        for team in teams:
            if team['name'].lower() == TEAM_NAME.lower() or team['slug'].lower() == TEAM_NAME.lower():
                return team['id']

        page += 1
        time.sleep(1) # Respect rate limits

    print(f"Team '{TEAM_NAME}' not found.")
    return None

def get_team_repositories():
    """Get all repositories the team has access to."""
    team_id = get_team_id()
    if not team_id:
        print("Cannot fetch repositories without team ID")
        return []

    repos = []
    page = 1

    print(f"Fetching repositories for team ID: {team_id}")

    while True:
        url = f'https://api.github.com/teams/{team_id}/repos?per_page=100&page={page}'
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Error fetching repositories: {response.status_code}")
            print(response.json())
            break

        page_repos = response.json()
        if not page_repos:
            break

        repos.extend(page_repos)
        print(f"Fetched page {page}, got {len(page_repos)} repositories")
        page += 1

        # Respect GitHub's rate limits
        if page > 1:
            time.sleep(1)

    return repos

def get_branch_counts(repo_list):
    """Get the exact number of branches for each repository."""
    results = []

    for i, repo in enumerate(repo_list):
        repo_full_name = repo['full_name']
        repo_name = repo['name']

        try:
            # Count all branches by paginating through them
            all_branches = []
            page = 1

            while True:
                branches_url = f'https://api.github.com/repos/{repo_full_name}/branches?per_page=100&page={page}'
                response = requests.get(branches_url, headers=headers)

                if response.status_code != 200:
                    print(f"Error fetching branches for {repo_name}: {response.status_code}")
                    break

                page_branches = response.json()
                if not page_branches:
                    break

                all_branches.extend(page_branches)
                page += 1

                # Check if we need to fetch more pages
                if len(page_branches) < 100:
                    break

                # Respect rate limits
                time.sleep(1)

            total_branches = len(all_branches)

            results.append({
                'Repository': repo_name,
                'Total Branches': total_branches
            })

            print(f"Processed {i+1}/{len(repo_list)}: {repo_name} - {total_branches} branches")

            # Respect GitHub's rate limits
            time.sleep(1)

        except Exception as e:
            print(f"Error processing {repo_name}: {str(e)}")
            time.sleep(3) # Longer wait if there's an error

    return results

def main():
    print(f"Starting analysis for team: {TEAM_NAME} in organization: {ORGANIZATION}")

    # Get all repositories for the team
    repos = get_team_repositories()
    print(f"Found {len(repos)} repositories for the team")

    if not repos:
        print("No repositories found. Please check your team name, organization name, and token.")
        return

    # Get accurate branch counts for each repository
    print(f"Fetching branch data for {len(repos)} repositories...")
    results = get_branch_counts(repos)

    # Create DataFrame for output
    df = pd.DataFrame(results)

    # Save results to CSV
    df.to_csv('github_branch_analysis.csv', index=False)

    # Print table to console
    print("\nRepository Analysis Results:")
    print(tabulate(df, headers='keys', tablefmt='grid'))

    print(f"\nFull results saved to github_repo_analysis.csv")
    print(f"Analysis complete for {len(results)}/{len(repos)} repositories.")

if __name__ == "__main__":
    main()
