## Analyzing and Comparing CO2 Emissions, Average Rainfall, and Average Temperature and Effects on Crop Production in the United States, Brasil, China, and India, 2000-Present:


### Background



### Data Sources



### Methods
In this section, we outline our general ideas for our pipeline. 

Proposed pipeline:

<p align="center"><img width="497" alt="Screen Shot 2022-05-23 at 5 19 04 PM" src="https://user-images.githubusercontent.com/48080946/169914060-2b50c0b0-1440-4c7a-8b8f-01d441c42e8e.png"></p>

- Data Storage: 
  - We will be using AWS S3 as our main stroge for this project. We will create a folder inside our bucket for raw data and cleaned data. Our database will be PostgreSQL and hosted through an instance in AWS. 

- Data Processing: 
  - Most of our data cleaning and processing will be done in Python. We will also explore Pyspark and SparkSQL. From there, we will load the cleaned data to the database in stage_schema. We will also build SQL scripts to build our dimension and fact tables.  We may use dimensional modeling and design a star schema with a bunch of dimension and fact tables. We may decide to change this design later. 

- Data Orchestration: 
  - We will explore using Apache Airflow to fully automate our pipeline. We might create an EC2 instance and install airflow inside a Docker container. As we build our pipeline, we may begin by parsing out each individual piece we are building and using cron jobs to automate some of these pieces. Eventually, we will incorporate Airflow for full automation. 






