# Data Modeling with Postgres 

### Summary

A music streaming app, Sparkify wants to analyze the data on songs and user activity. The analytics team wants to know what songs users are listening to. Currently, thre is no easy way to query the data, which resides in a directory of JSON logs about user activity, as well as a directory with JSON metadata on the songs.

We need to create a Postgres database with tables designed to optimize queries on song play analysis. Also create a database schema and ETL pipeline for this analysis. Test the database and ETL pipeline by running queries from the analytics team.

### Project Description
Data Model with Postgres and build an ETL pipeline using Python. We will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.


### Pre-Requisites
* PostgreSQL
* Ensure psycopg2, pandas, glob are installed in a python3 environment
* Jupyter Notebook (Testing)

### How to Run & Test

* Use RunETL.ipynb (Jupyter Notebook) to Setup Database and Run ETL Pipeline. 
* Run all the cells of test.ipynb (Jupyter Notebook) to verify the music database is setup correctly.
* Restart the kernels to enable re-runs.

### Files
| Filename |  |
| ------ | ------ |
| sql_queries.py | PostgreSQL queries to create & drop tables, Insert data into the tables, etc. | 
| create_tables.py | Ensures clean database, connection and create tables. |
| etl.py: | Extract (Read song & log datafiles), Transform (Process information) & Load(insert data into  tables). |
| RunETL.ipynb | Setup Database and Run ETL Pipeline |
| test.ipynb | A test bench to verify the music database tables after ETL. |
| etl.ipynb | A guide to help create & test the coding parts of Extract Transform Load. |
| data | Contains songs & log data json files. |


### Acknowledgement
Author: Hari Raja
Framework: Udacity
Date: Apr 5 2020
