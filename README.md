# Stale_Data_Collection

Running in virtual env is recommended.
programming language: python

Installations:
--------------------------------------------------------
> pip install requests pandas tabulate tqdm openpyxl

Steps:
--------------------------------------------------------
1. larrgerepobranches.py creates a csv file with repository_names, no.of branches, no.of stale branches, pass the created csv file to step 2 
> the csv sheet consists of repositoty name, no.of branches, no.of stale branches.
2. largefileofstale.py creates a json file with stale information, and a xlsx format sheet containing tabs(took 2 complete days to run), give the created json file to step 3
3. jsontocsv.py creates a csv file out of json file, give the created csv file to step 4.
4. formatting.py gives coloring to variate different repositories, generates a csv file.

Make sure to include your organistation # github's token, name and team name.

we get the stale data information like:
1. repository it belongs to
2. stale branch name
3. its last commited date
4. branch it is merged to.
