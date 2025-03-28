# Stale_Data_Collection

Running in virtual env is recommended.
programming language: python

Installations:
--------------------------------------------------------
> pip install requests pandas tabulate tqdm openpyxl

Steps:
--------------------------------------------------------
i) larrgerepobranches.py creates a csv file with repository_names, no.of branches, no.of stale branches, pass the created csv file to step 2 
> the csv sheet consists of repositoty name, no.of branches, no.of stale branches.
ii) largefileofstale.py creates a json file with stale information, and a xlsx format sheet containing tabs(took 2 complete days to run), give the created json file to step 3
iii) jsontocsv.py creates a csv file out of json file, give the created csv file to step 4.
iv) formatting.py gives coloring to variate different repositories, generates a csv file.

Make sure to include your organistation github's token, name and team name.

we get the stale data information like:
> repository it belongs to
> stale branch name
> its last commited date
> branch it is merged to.
