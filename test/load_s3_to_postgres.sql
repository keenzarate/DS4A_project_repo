-- install extension
create extension aws_s3 cascade

-- create a schema 
create schema test_data

-- create a table inside the schema and add the column names (the number of columns here should match your data)
-- you can force data type here also
create table test_data.raw_crops(
	entity text,
	code text,
	prod_year int, 
	production int
);


-- call function to access s3 data and load into the table we want
-- for documentation (https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.html)
select aws_s3.table_import_from_s3(
-- fill in all required parameters listed below

-- schema_name.table_name
-- all the columns list here ('entity, code, prod_year, production'), 
-- '(format csv, header true)',
-- s3 bucket name,
-- folder/filename,
-- region,
-- access_key, 
-- secret_key
)

