# Totesys Data Pipeline
Data pipeline to move data from Totesys OLTP database to OLAP database for BI.

## Description
Move data from a relational OLTP database into a OLAP database stored in star schema every 20 minutes. The data pipeline consists of three ETL stages.

## Pipeline Diagram
![alt text](image-pipeline_diagram.png)

### Extract
Take the data from the OLTP database and store the data in JSON format in an S3 bucket
### Tranform
Transform the JSON data into a star schema and store in parquet format
### Load
Loads the data into the end OLAP data warehouse

## Date Warehouse Schema
The final schema of the dataware is a star schema made up of 3 facts tables with 8 dimension tables
![alt text](image-warehouse_schema.png)

## Built With
### AWS Cloud
- Lambda
- Step-function
- CloudWatch
- EventBridge
- S3
- RDS
- Quicksight
- SNS

### Terraform
- Build cloud infrastructure

### Python
- Code for Lambda functions

## Roadmap
- [ ] Upload data into the final warehouse
- [ ] Add business intelligence 
- [ ] Refactor
- [ ] Optimise


## Authors
- Heiman
- Leonette
- Michael
- Mostyn
- Nicholas