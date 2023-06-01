# Assignment for FXStreet

Execution instuctions:

    In order to execute, run sudo docker-compose build,
    then run sudo docker-compose up -d

    To execute the pipeline, do a get request to localhost:8080/api/v1/main
    If everything went correct you should see a json that contains the result for the 
    conversion rate per step and week.

In the notebook.py you can find a python Inotebook that explains step by step all answers
until step 3 (included).

Unitary tests have been done with pytest and are found in utils.py

In order to execute them use pytest utils.py