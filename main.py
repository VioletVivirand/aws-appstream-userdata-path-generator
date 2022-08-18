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

def export_homefolder_report(buckets_detail_homefolder, users_detail):
    # Prepare data for CSV export
    # Header = 'User Name', 'First Name', 'Last Name', Home Folder URI (<Region Name>), Home Folder URI (<Region Name>), ... 
    header = ['User Name', 'First Name', 'Last Name']

    for bucket_detail_homefolder in buckets_detail_homefolder:
        bucket_region = bucket_detail_homefolder['Region']
        header.append(f'Home Folder URL ({bucket_region})')

    # Row = '<UserName>', '<FirstName>', '<LastName>', 'S3 URI', 'S3 URI', ... 
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

def export_recording_report():
    pass

def main():
    # Get clients
    s3 = boto3.client('s3')
    sts = boto3.client('sts')
    appstream = boto3.client('appstream')

    # Get AWS Account ID
    ACCOUNT_ID = get_account_id(sts)

    # Get users' information from User Pool
    users_detail = get_users_detail(appstream)

    # Get all buckets' name
    buckets_name = get_buckets_name(s3)

    # Filter out bucket used for storing home folders and get additional information
    buckets_detail_homefolder = get_buckets_detail_homefolder(buckets_name, ACCOUNT_ID)

    # Export report of users' home folder paths
    export_homefolder_report(buckets_detail_homefolder, users_detail)


if __name__ == "__main__":
    main()