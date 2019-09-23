from _future_ import print_function
from datetime import datetime, timedelta
import psycopg2
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload
import io
import csv
import pandas as pd
import logging
import pickle
import os.path
from pathlib import Path
​
​
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
​
from datetime import datetime, date
import boto3
​
​
logger=logging.getLogger(_name_)
timestamp=datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
​
​
#todo: Convert prints to loggers
def download_csv_and_move_to_s3():
    creds = None
    # Setup the Drive v3 API
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time. Just need to download configuration.json and after the first call, token.pickle
    #will be created automatically
    SCOPES = ['https://www.googleapis.com/auth/drive']
    #todo: Convert path of credentials.json to dynamic, get from os
    store = file.Storage('/Users/kushalwalia/Documents/dev_local/credentials.json')
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/Users/kushalwalia/Documents/dev_local/credentials.json', SCOPES)
            creds = tools.run_flow(flow, store)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
​
    service = build('drive', 'v3', credentials=creds)
​
​
    file_id = '1prbmMokdZvgSSnclAK31KvbxrVvjOvyTTOoxW0Q0srs'
    request = service.files().export_media(fileId=file_id,
                                                 mimeType='text/csv')
    fh = io.BytesIO() # Using an in memory stream location
​
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
​
    logger.info(u"Uploading to S3")
    s3 = boto3.resource('s3')
    s3.Bucket('bucket-name').put_object(
        Key="{}{}{}".format('sheet_name_to_move_',timestamp,'.csv'),
        Body=fh.getvalue(),
        ACL='public-read'
    )
​
    logger.info(u"Uploaded to S3")
    print ('file moved to s3 via memory using BytesIO')
    fh.close()  # close the file handle to release memory as the file was downloaded in memory
​
def get_query_files(sql_file_name):
​
    directory = os.getcwd()
    p=Path(directory)
    logger.info('posix path is: ' , p)
    return list(p.glob(sql_file_name))
​
def read_query(sql_file_name):

    files = get_query_files(sql_file_name)
    logger.info('file is',files)
    logger.info('in read query method')
    i=0
    for fileobj in files:
        with open(str(fileobj), 'r') as handle:
            contents = handle.read()
            i+=1
            logger.info('in read_query contenats loop',i, type(contents))
            yield (fileobj, contents)
​
def move_from_s3_to_redshfit():
​
    truncate_query = read_query('truncate_query.sql')
    load_query=read_query('copy_from_s3_to_redshift.sql')
    queries = [truncate_query,load_query]

    #Todo: Use airflow dynamic postgres_hook
    host='bla-bla-bla.blagtop98.us-west-1.redshift.amazonaws.com'
    port='5439'
    dbname='dbname'
    user='username'
    password='pwd'
​
    con=psycopg2.connect(dbname=dbname,host=host,port=port,user=user,password=password)

    for sql_file_name in queries:
        for fileobj, sql_file_content in sql_file_name:
            if fileobj.stem == 'copy_from_s3_to_redshift':
                sql_file_content = sql_file_content.format(filetimestamp=timestamp)
            elif fileobj.stem == 'truncate_query':
                pass
            else:
                print ('query not recognized')
​
            if con.closed==0:
                print("succesfully connected")
                cursor=con.cursor()
                print (sql_file_content)
                cursor.execute(sql_file_content)
                con.commit()
                print(fileobj.stem, "query successfully executed")
    con.close()
    print("connection closed")
​
​
download_csv_and_move_to_s3()
move_from_s3_to_redshfit()
