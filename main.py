import boto3
import re
import hashlib
import urllib.parse
import csv

# Get clients
s3 = boto3.client('s3')
sts = boto3.client('sts')
appstream = boto3.client('appstream')

# Get AWS Account ID
ACCOUNT_ID = sts.get_caller_identity()['Account']

# Get all bucket names, filter out the bucket for
# 1. AppStream Home Folder
buckets = [doc['Name'] for doc in s3.list_buckets()['Buckets']]

p_homefolder = r"appstream2-36fb080bb8-(\S+)-"+ ACCOUNT_ID +"$"
buckets_detail_homefolder = [{
        "BucketName": re.search(p_homefolder, bucket).group(),
        "Region": re.search(p_homefolder, bucket).group(1)
    } for bucket in buckets if re.search(p_homefolder, bucket)]

# [TODO] 2. AppStream Session Recording Bucket

# Get users' information from User Pool
users = appstream.describe_users(AuthenticationType="USERPOOL")['Users']
users_detail = [{
    'Hash': user['Arn'].split('/')[-1],
    'UserName': user['UserName'],
    'FirstName': user['FirstName'],
    'LastName': user['LastName'],
    } for user in users]

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
with open('output.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile)
    spamwriter.writerow(header)

    for row in rows:
        spamwriter.writerow(row)
