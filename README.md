Here we have some simpler question we would like you to answer by writing some small Spark Job(s) in any language you would prefer.

- a) Create the dataframe with joins with master (optimization using broadcasts)
- b) Based on the dataset find the Airlines with the least delay (sorting and selecting the top)
- c) Multiple groupings like which Airline has most flights to New York (uses reduce and combine operators)
- d) Secondary sorts like which airlines arrive the worst on which airport and by what delay
- e) Custom partitioners using airline Id (in combination with - d)

We would suggest you to use the data for January, which will be around 100+MB deflated. 
On our test these should not last much longer than a minute to execute on local machine as e.g. your laptop. 
