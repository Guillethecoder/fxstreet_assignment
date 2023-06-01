# Assignment for FXStreet

# Prerequisites
Before running the code, ensure that you have docker engine installed. Also it will be necessary to
have pytest install in order to run unitary tests

# Execution instuctions:

In order to execute, run
```
sudo docker-compose build,
sudo docker-compose up -d
```
To execute the pipeline, do a get request to 
    localhost:8080/api/v1/main
If everything went correct you should see a json that contains the result for the 
conversion rate per step and week.



In the **notebook.py** you can find a python Inotebook that explains step by step all answers
until exercise 3 (included).

Unitary tests have been done with pytest and are found in utils.py

In order to execute them run
```
pytest utils.py
```
I chose step 4 to complete, and didn't complete step 5 due to lack of time.