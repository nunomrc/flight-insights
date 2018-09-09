## Summary

This code does the following:

- Creates a dataframe with joins with master
- Based on the dataset, finds the Airlines with the least delay
- Exercises multiple groupings like which Airline has most flights to New York
- Exercises secondary sorts like which airlines arrive the worst on which airport and by what delay
- Exercises custom partitioners using airline Id (optimised version of the previous topic)

Currently using the data from January, downloaded from here:
https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time

All the following files (source data) are expected to be in the same folder:

- On_Time_On_Time_Performance_2018_1.csv
- L_AIRLINE_ID.csv
- L_AIRPORT_ID.csv
- L_CITY_MARKET_ID.csv

## Running the spark App: 

This code was tested with Apache Spark version 2.3.1, in local mode only so far.
It can be executed using the following command:

```
$ spark-submit --class insights.InsightsApp --master local[6] --num-executors 6 --driver-memory 2G target/scala-2.11/flight-insights_2.11-0.1.jar /data/source/folder
```

This App in local mode is currently taking the following time to execute in a regular laptop (output from `time` bash command):

```
real	0m28.248s
user	1m39.277s
sys	0m5.310s
```

Results are written in CSV to a folder named `results`, from where to app is executed.
Performance measurements are taken for each query and output during the execution of the job.
