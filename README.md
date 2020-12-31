# Project structure

* *data* directory - raw data from kaggle 
* *hql/tables* - hql scripts to define the structure of tables in Hive
* *hql/selects* - hql scripts for daily report
* *results* - this directory contains all csv outputs for daily and cumulative analysis
* *analysis.py* - pyspark code for cumulative analysis
* *cumulative_report.ipynb* - python notebook with cumulative analysis
* *example_of_daily_analysis.ipynb* - python notebook with examples of outputs from daily report
* *daily_report.sh* - shel script to run daily report 