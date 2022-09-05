import fire
import os
import boto3
import re
import hashlib
import urllib.parse
import csv

def get_account_id(client_sts) -> str:
    account_id = client_sts.get_caller_identity()['Account']

    return account_id

def get_users_detail(client_appstream) -> dict:
    users = client_appstream.describe_users(AuthenticationType="USERPOOL")['Users']
    users_detail = [{
        'Hash': user['Arn'].split('/')[-1],
        'UserName': user['UserName'],
        'FirstName': user['FirstName'],
        'LastName': user['LastName'],
        } for user in users]
    
    return users_detail

def get_buckets_name(client_s3) -> list:
    buckets_name = [doc['Name'] for doc in client_s3.list_buckets()['Buckets']]

    return buckets_name

def get_buckets_detail_homefolder(buckets_name, account_id) -> list:
    p_homefolder = r"appstream2-36fb080bb8-(\S+)-"+ account_id + r"$"

    buckets_detail_homefolder = [{
            "BucketName": re.search(p_homefolder, bucket_name).group(),
            "Region": re.search(p_homefolder, bucket_name).group(1)
        } for bucket_name in buckets_name if re.search(p_homefolder, bucket_name)]
    
    return buckets_detail_homefolder

def generate_homefolder_report(buckets_detail_homefolder, users_detail):
    # Prepare data for CSV export
    # Header = 'User Name', 'First Name', 'Last Name', Home Folder URL (<Region Name>), Home Folder URI (<Region Name>), ... 
    header = ['User Name', 'First Name', 'Last Name']

    for bucket_detail_homefolder in buckets_detail_homefolder:
        bucket_region = bucket_detail_homefolder['Region']
        header.append(f'Home Folder S3 URL ({bucket_region})')

    # Row = '<UserName>', '<FirstName>', '<LastName>', 'S3 URL', 'S3 URL', ... 
    rows = []

    for user_detail in users_detail:
        row = [user_detail['UserName'], user_detail['FirstName'], user_detail['LastName']]

        # URL for Home Folders
        for bucket_detail_homefolder in buckets_detail_homefolder:
            # S3 URL for Home Folder = 'https://s3.console.aws.amazon.com/s3/buckets/<Bucket Name>?prefix=user/userpool/<User ID SHA256 Hash>'
            bucket_name = bucket_detail_homefolder['BucketName']
            user_id_sha256_hash = hashlib.sha256(str.encode(user_detail['UserName'])).hexdigest()
            params = urllib.parse.urlencode({'prefix': f'user/userpool/{user_id_sha256_hash}/'})
            bucket_URL = f'https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}?{params}'
            row.append(bucket_URL)
        
        rows.append(row)

    # Export CSV file
    with open('report_homefolder.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(header)

        for row in rows:
            spamwriter.writerow(row)

def generate_sessionrecording_report(bucket_name_sessionrecording, stack_name, fleet_name, users_detail):
    # Prepare data for CSV export
    # Header = 'User Name', 'First Name', 'Last Name', Session Recording URL (<Region Name>), Home Folder URI (<Region Name>), ... 
    header = ['User Name', 'First Name', 'Last Name', 'Session Recording S3 URL']

    # Row = '<UserName>', '<FirstName>', '<LastName>', 'S3 URL', 'S3 URL', ... 
    rows = []

    for user_detail in users_detail:
        row = [user_detail['UserName'], user_detail['FirstName'], user_detail['LastName']]

        # URL for Session Recording
        # S3 URL for Session Recording = https://s3.console.aws.amazon.com/s3/buckets/<Bucket Name>?prefix=<Stack Name>/<Fleet Name>/<User ARN Hash>/
        user_arn_hash = user_detail['Hash']
        params = urllib.parse.urlencode({'prefix': f'{stack_name}/{fleet_name}/{user_arn_hash}/'})
        bucket_URL = f'https://s3.console.aws.amazon.com/s3/buckets/{bucket_name_sessionrecording}?{params}'
        row.append(bucket_URL)
    
    rows.append(row)

    # Export CSV file
    with open('report_sessionrecording.csv', 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile)
        spamwriter.writerow(header)

        for row in rows:
            spamwriter.writerow(row)

def generate_s3log_report(database, table, users_detail, datestart=None, dateend=None):
    import awswrangler as wr
    import pandas as pd

    # Generate user name hash table for replacing
    hash_username = {hashlib.sha256(str.encode(user_detail['UserName'])).hexdigest():user_detail['UserName'] for user_detail in users_detail}
    # Generate user detail DataFrame for merging DataFrames
    users_df = pd.DataFrame(users_detail)
    users_df.drop("Hash", axis=1, inplace=True)

    if not datestart:
        # Search data before dateend
        date_between_sql = f"""parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z') <= parse_datetime(
            '{dateend}',
            'yyyy-MM-dd HH:mm:ss Z'
        )"""
    elif not dateend:
        # Search data after datestart
        date_between_sql = f"""parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z') >= parse_datetime(
            '{datestart}',
            'yyyy-MM-dd HH:mm:ss Z'
        )"""
    else:
        # Search data between datestart and dateend
        date_between_sql = f"""parse_datetime(requestdatetime, 'dd/MMM/yyyy:HH:mm:ss Z') BETWEEN parse_datetime(
            '{datestart}',
            'yyyy-MM-dd HH:mm:ss Z'
        ) AND parse_datetime(
            '{dateend}',
            'yyyy-MM-dd HH:mm:ss Z'
        )"""

    # Prepare SQL query for reading only PUTOBJECT and BATCH.DELETE operationos
    SQL = f"""SELECT *
    FROM "{database}"."{table}"
    WHERE {date_between_sql}
        AND operation IN (
		    'REST.PUT.OBJECT',
		    'REST.COPY.OBJECT',
		    'REST.COPY.OBJECT_GET',
		    'REST.BATCH.DELETE'
	    );"""

    # Read access log via Data Wrangler as DataFrame
    df = wr.athena.read_sql_query(SQL, database=database)

    # Regex, group 1: user/userpool/, group 2: username hash
    r = r"(^user\/userpool\/)(\w+)(\/){1}"
    # Create a new userpool username column, extract username hash from 'key' column
    # and try to match with username hash table (hash_username)
    df.loc[:, 'UserName'] = df.loc[:, 'key'].str.extract(r).iloc[:, 1].replace(hash_username)
    # Merge with the rest user details
    df = pd.merge(df, users_df, how='left', on=['UserName'])
    # Convert the requestdatetime as datetime for sorting
    df.loc[:, 'requestdatetime'] = pd.to_datetime(df.loc[:, 'requestdatetime'], format="%d/%b/%Y:%H:%M:%S %z")

    # Export
    df_export = df.loc[:, ['key', 'requestdatetime', 'UserName', 'FirstName', 'LastName']]\
        .sort_values(by=['requestdatetime'], ascending=False)\
        .reset_index(drop=True)
    df_export.to_csv('report_s3log.csv')

def export_homefolder_report(bucket=None):
    # Get clients
    s3 = boto3.client('s3')
    sts = boto3.client('sts')
    appstream = boto3.client('appstream')

    # Get AWS Account ID
    ACCOUNT_ID = get_account_id(sts)

    # Get users' information from User Pool
    users_detail = get_users_detail(appstream)

    if not bucket:
        print("Bucket name not provided, try to get it automatically.")        

        # Get all buckets' name
        buckets_name = get_buckets_name(s3)

        # Filter out bucket used for storing home folders and get additional information
        buckets_detail_homefolder = get_buckets_detail_homefolder(buckets_name, ACCOUNT_ID)

    # Export report of users' home folder paths
    generate_homefolder_report(buckets_detail_homefolder, users_detail)

def export_sessionrecording_report(bucket=None, stack=None, fleet=None):
    if not bucket:
        print("Please provide Bucket name for storing session recording files.")
        print("by adding \"--bucket\" option.")
        print("Exit...")
        os.exit(1)
    
    if not stack:
        print("Please provide Stack name with session recording feature enabled.")
        print("by adding \"--stack\" option.")
        print("Exit...")
        os.exit(1)
    
    if not fleet:
        print("Please provide Fleet name with session recording feature enabled.")
        print("by adding \"--fleet\" option.")
        print("Exit...")
        os.exit(1)

    # Get client
    appstream = boto3.client('appstream')

    # Get users' information from User Pool
    users_detail = get_users_detail(appstream)

    generate_sessionrecording_report(
        bucket_name_sessionrecording=bucket,
        stack_name=stack,
        fleet_name=fleet,
        users_detail=users_detail)

def export_s3log_report(database, table, datestart=None, dateend=None):
    if not database:
        print("Please provide Glue Data Catalog database name for storing S3 access logs")
        print("by adding \"--database\" option.")
        print("Exit...")
        os.exit(1)

    if not table:
        print("Please provide Glue Data Catalog table name for storing S3 access logs")
        print("by adding \"--table\" option.")
        print("Exit...")
        os.exit(1)

    if not datestart and not dateend:
        print("Please provide at least one date reference in ISO format for searcing data")
        print("by adding \"--datestart\" or \"--dateend\" option.")

    # Get client
    appstream = boto3.client('appstream')

    # Get users' information from User Pool
    users_detail = get_users_detail(appstream)

    generate_s3log_report(
        database=database,
        table=table,
        users_detail=users_detail,
        datestart=datestart,
        dateend=dateend
    )



def main():
    fire.Fire({
        'home-folder': export_homefolder_report,
        'session-recording': export_sessionrecording_report,
        's3-accesslog': export_s3log_report,
    })

if __name__ == "__main__":
    main()