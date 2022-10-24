import fire
import sys
from loguru import logger
import boto3
import re
import hashlib
import urllib.parse
import csv

def set_logger(debug):

    # DEFAULT_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "\
    # "<level>{level: <8}</level> | "\
    # "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

    INFO_FORMAT = "<level>{message}</level>"

    if debug:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add("debug.log", rotation="1 MB", level='DEBUG') 
        logger.info("Debug mode enabled.")
    else:
        logger.remove()
        logger.add(sys.stderr, format=INFO_FORMAT, level="INFO")
        

def get_account_id(client_sts) -> str:
    logger.debug("Getting AWS Account ID")

    account_id = client_sts.get_caller_identity()['Account']
    logger.debug(f"AWS Account ID = {account_id}")

    return account_id

def get_users_detail(client_appstream) -> dict:
    logger.debug("Getting Userpool Information")
    users = client_appstream.describe_users(AuthenticationType="USERPOOL")['Users']
    users_detail = [{
        'Hash': user['Arn'].split('/')[-1],
        'UserName': user['UserName'],
        'FirstName': user['FirstName'],
        'LastName': user['LastName'],
        } for user in users]
    
    return users_detail

def get_buckets_name(client_s3) -> list:
    logger.debug("Getting buckets name")
    buckets_name = [doc['Name'] for doc in client_s3.list_buckets()['Buckets']]
    logger.debug(f"Bucket name = {buckets_name}")

    return buckets_name

def get_buckets_detail_homefolder(buckets_name, account_id) -> list:
    logger.debug("Getting home folder buckets name")
    logger.debug(f"buckets_name = {buckets_name}, account_id = {account_id}")
    p_homefolder = r"appstream2-36fb080bb8-(\S+)-"+ account_id + r"$"

    buckets_detail_homefolder = [{
            "BucketName": re.search(p_homefolder, bucket_name).group(),
            "Region": re.search(p_homefolder, bucket_name).group(1)
        } for bucket_name in buckets_name if re.search(p_homefolder, bucket_name)]
    logger.debug("Home folder bucket names")
    logger.debug(buckets_detail_homefolder)
    
    return buckets_detail_homefolder

def generate_homefolder_report(buckets_detail_homefolder, users_detail):
    logger.debug("Generating home folder report")
    # Prepare data for CSV export
    # Header = 'User Name', 'First Name', 'Last Name', Home Folder URL (<Region Name>), Home Folder URI (<Region Name>), ... 
    header = ['User Name', 'First Name', 'Last Name']

    for bucket_detail_homefolder in buckets_detail_homefolder:
        bucket_region = bucket_detail_homefolder['Region']
        header.append(f'Home Folder S3 URL ({bucket_region})')
        logger.debug(f"Added region {bucket_region} to header")

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
    
    logger.info("Report exported to report_homefolder.csv")

def generate_s3log_report(database, table, users_detail, datestart=None, dateend=None):
    logger.debug("Generating S3 access log report")
    logger.debug(f"database = {database}, table = {table}, users_detail = {users_detail}, datestart = {datestart}, dateend = {dateend}")
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
		    'REST.BATCH.DELETE',
            'REST.DELETE.OBJECT',
		    'REST.POST.MULTI_OBJECT_DELETE'
	    );"""
    logger.debug(f"SQL Query = \n{SQL}")

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
    df_export = df.loc[:, ['bucket_name', 'key', 'operation', 'requestdatetime', 'UserName', 'FirstName', 'LastName']]\
        .sort_values(by=['requestdatetime'], ascending=False)\
        .reset_index(drop=True)
    
    df_export.to_csv('report_s3log.csv')

    logger.info("Report exported to report_s3log.csv")

def export_homefolder_report(bucket: str = None, debug: bool = False):
    """Generate report of each user's AppStream Home Folder path in S3 bucket
    """

    # Set Loguru logger
    set_logger(debug=debug)
    logger.info("Exporting Home Folder report...")

    # Log the parameters in debug mode
    logger.debug(f"bucket = {bucket}")

    # Get clients    
    s3 = boto3.client('s3')

    sts = boto3.client('sts')

    appstream = boto3.client('appstream')

    # Get AWS Account ID
    ACCOUNT_ID = get_account_id(sts)

    # Get users' information from User Pool
    users_detail = get_users_detail(appstream)

    if not bucket:
        logger.warning("Bucket name not provided, try to get it automatically.")        

        # Get all buckets' name
        buckets_name = get_buckets_name(s3)

        # Filter out bucket used for storing home folders and get additional information
        buckets_detail_homefolder = get_buckets_detail_homefolder(buckets_name, ACCOUNT_ID)

    # Export report of users' home folder paths
    generate_homefolder_report(buckets_detail_homefolder, users_detail)

def export_s3log_report(database: str, table: str, datestart: str = None, dateend: str = None, debug: bool = False):
    """Generate report of each user's S3 access log
    """

    # Set Loguru logger
    set_logger(debug=debug)
    logger.info("Exporting S3 access log report...")

    # Log the parameters in debug mode
    logger.debug(f"database = {database}, table = {table}, datestart = {datestart}, dateend = {dateend}")

    if not datestart and not dateend:
        logger.info("Please provide at least one date reference in ISO format for searcing data\n"
                    "by adding \"--datestart\" or \"--dateend\" option.\n"
                    "Exit...")
        sys.exit(1)

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
        's3-accesslog': export_s3log_report,
    })

if __name__ == "__main__":
    main()