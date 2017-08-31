<h1>This is based on csv data available on Kaggle.</h1>

<h1>Goal</h1>  Load csv into spark datframes and then transform and save in Redshift. Data from Redshift can be easily plugged into Tableau or other software for createing nice charts and dashboards.


<h1>How to run</h1>

Rquires spark-2.0.0

./bin/spark-submit --packages com.databricks:spark-redshift_2.10:3.0.0-preview1,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0  --jars postgresql-42.1.4.jar,aws-java-sdk-1.11.179.jar  /location/of/run.py pc_import_2014_2015.csv


<h1>A couple of screenshots using AWS Quicksight</h1>

1. ![Overview](https://github.com/phanisaripalli/import-and-export-by-india/blob/master/quicksight_1.png)

2. ![Dashboard](https://github.com/phanisaripalli/import-and-export-by-india/blob/master/quicksight_2.png)
