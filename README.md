# google_sheets_to_redshift
In this repo, we move data from google sheets to AWS redshift


This data pipeline works in 4 steps:

1. Make the google api call.
2. Download the google sheet you want to move to the data warehouse in memory
  2.1 We use BytesIO to read the file in memory. 
3. Move the file to AWS S3 for transferring to AWS Redshift and backup
4. Move the data to AWS Redshift via the copy command.

Data pipelines are basically of two kinds: Full load and Incremental load. In this pipeline, we do a full load for which we truncate the table everytime we load it again.

The truncate and copy commands are stored in seperate sql files. We read these files using the posix path when the file names are sequentially passed into the read_query() method. Once the .sql file is read, the we yeild them, open the file and read its contents. 

If the file contains the copy command, we append the timestamp of runtime to the file name being unloaded.

I would be very happy to receive feedback on how to improve this code.

Thanks and Enjoy!!

