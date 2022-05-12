# ny-eviction-dashboard

Inspired by https://github.com/ilya-galperin/SF-EvictionTracker

The above repo is for SF, but this one is for NYC. This project is a means for me to learn airflow. Still a work in progress. 

## How to run

1. Clone into a repo that is in the 'dags' folder in same path as `AIRFLOW_HOME` (environment variable stored in `.bash_profile` or `.bashrc file)`
2. To run, install airflow in a virtualenv and run in bash
  ```$source env/bin/activate && airflow webserver```
  in one terminal
  ```$source env/bin/activate && airflow scheduler```
  in another.

It is also possible to run them as daemons if we want to run on one VM/terminal (using systemctl).

(Airflow opens on localhost:8080)

To run on AWS follow these instructions for EC2: https://medium.com/@abraham.pabbathi/airflow-on-aws-ec2-instance-with-ubuntu-aff8d3206171, but the data is small enough to just run on local. 

## Description
I've added a dag for a full download of the API (`ny_evictions_full_load_dag.py`), but not the incremental (daily) load yet. 
Steps:
1. Download from SODA API to S3 (data from: https://data.cityofnewyork.us/City-Government/Evictions/6z8x-wfk4)
2. Load from S3 to postgres database
3. Create a simple Dash app from the postgres database

## To do:
1. Add an incremental load dag to load new API hits daily
2. Configure Dash app on AWS (using Elastic Beanstalk)
3. Add more visualizations such as graphs or correlations with zip-code wise demographic data or building specific data in the NYC PLUTO dataset
