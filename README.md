# Data Warehouse

This is an example ETL pipeline using AWS S3 and Redshift, and demonstrating basic patterns of Infrastructure as Code (IaC).

The data reflects usage of an imaginary music streaming service called Sparkify. There are records for songs (from the Million Song Dataset) and songplays (produced by an event generator). Both are stored in JSON format in S3 buckets. The IaC pipeline begins by creating database tables in Redshift for both staging and querying the data. The pipeline copies data from the S3 buckets into staging tables, then transforms the staging data and copies it into fact and dimension tables for querying.

This code depends on IaC code in another repostory which automatically generates a Redshift cluster and an IAM role with relevant permissions.

## Files
* `dwh.cfg` configuration values necessary for communicating with AWS, including cluster address (`HOST`) and user role/permissions (`ARN`)
* `sql_queries.py` SQL queries stored as strings to be imported into other processes. Includes queries for dropping and creating tables, and copying data from one source to another
* `create_tables.py` drops existing tables, and creates new tables
* `etl.py` copies data from S3 into Redshift staging tables, then transforms staging data into fact/dimension tables

## process
* edit `dwh.cfg` to include necessary values
* run `python create_tables.py`
* run `python etl.py`