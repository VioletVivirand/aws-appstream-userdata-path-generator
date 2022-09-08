# Amazon AppStream User Data Path Generator <!-- omit in toc -->

[Amazon AppStream 2.0](https://aws.amazon.com/appstream2/) is a good, dynamic DaaS (Desktop as a Service) solution, if you feel Amazon Workspaces is expensive, give AppStream 2.0 a try. AppStream 2.0 has lots of features to provide a cheaper desktop environment than other desktop services.

It also has some shortages, one of them is it's hard to manage the users' files stored in S3, which I think is a magic to make the service itself cheaper than others. Part of file key (like path) often contains the hashes transformed from user name (usually from E-mail) just like `7462108984f629db2ced1aeb2dc3e747e53a2e1c607059f72955ab864c724335`, and makes them hard to recognize who owns the file at first glance.

This repo tries to ship a little Python script to ease the burden by generating CSV reports that match the user resource and the user name. The script was tested with Python 3.8 but I believe it can be executed with Python 3.7, so it would be easy to be used in a [AWS Cloud Shell](https://aws.amazon.com/cloudshell/) environment. 

This script generates the reports as below:

* AppStream 2.0 user home folder paths in S3
* The file paths saved by [AppStream session recording solution](https://aws.amazon.com/blogs/security/how-to-record-video-of-amazon-appstream-2-0-streaming-sessions/)
* AppStream 2.0 users' S3 access log

## Table of contents <!-- omit in toc -->

- [Quickstart](#quickstart)
- [Usage](#usage)
  - [Provide AWS Configuration](#provide-aws-configuration)
  - [Python Environment Preparation](#python-environment-preparation)
  - [Generate the home folder path of AppStream 2.0 users](#generate-the-home-folder-path-of-appstream-20-users)
  - [Generate the session recording path or AppStream 2.0 users](#generate-the-session-recording-path-or-appstream-20-users)
  - [Generate the AppStream 2.0 S3 access logs](#generate-the-appstream-20-s3-access-logs)

## Quickstart

```bash
# Install required dependencies
pip install -r requirements

# To generate the home folder path of AppStream 2.0 users:
python main.py home-folder
# It will export the result to `report_homefolder.csv`.

# To generate the session recording path or AppStream 2.0 users:
python main.py session-recording \
  --bucket=<Bucket name> \
  --stack=<Stack name> \
  --fleet=<Fleet name>
# It will export the result to `report_sessionrecording.csv`.

# To generate the AppStream 2.0 S3 access logs:
python main.py s3-accesslog \
  --database=<Database name> \
  --table=<Table name> \
  --datestart="<ISO data format>" \
  --dateend="<ISO data format>"
# It will export the result to `report_s3log.csv`
```

## Usage

### Provide AWS Configuration

Follow the official documentation about the [Configuration basics](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html): execute `aws configure` to generate `credentials` and `config` files.

### Python Environment Preparation

Use `pip` to install dependencies:

```bash
pip install -r requirements.txt

# Some environment contains both Python 2 and Python 3, so it might be safer to explicitly install with Python 3
python3 -m pip install requirements.txt
```

### Generate the home folder path of AppStream 2.0 users

The preview of the report:

```
User Name,First Name,Last Name,Home Folder S3 URL (ap-northeast-1)
demo@example.com,Demo,User,https://s3.console.aws.amazon.com/s3/buckets/appstream2-36fb080bb8-ap-northeast-1-546614691476?prefix=user%2Fuserpool%2F7462108984f629db2ced1aeb2dc3e747e53a2e1c607059f72955ab864c724335%2F
```

To generate the home folder path of AppStream 2.0 users:

```bash
python main.py home-folder
```

The script will get the information about the account and region, then search the bucket that contains AppStream 2.0 home folder files. However, if the search function doesn't work correctly, you can still manually provide the bucket name by specifing the `--bucket` flag and the bucket name as value:

```bash
python main.py home-folder --bucket=<bucket name>

# Example
python main.py home-folder --bucket="appstream2-36fb080bb8-ap-northeast-1-546614691476"
```


### Generate the session recording path or AppStream 2.0 users

This function only works if you adopt the [AppStream session recording solution](https://aws.amazon.com/blogs/security/how-to-record-video-of-amazon-appstream-2-0-streaming-sessions/) provided on AWS Blog Post.

The preview of the report:

```
User Name,First Name,Last Name,Session Recording S3 URL
demo@example.com,Demo,User,https://s3.console.aws.amazon.com/s3/buckets/session-recording-bucket?prefix=stackname%2Ffleetname%2F84d6b9b2-2fee-429b-b4a4-7743d3ffe687%2F
```

Please provide all required variables: Bucket name that stores the session recording files, AppStream 2.0 Stack name and AppStream 2.0 Fleet name by specifying position arguments or flags:

```bash
python main.py session-recording \
  --bucket=<Bucket name> \
  --stack=<Stack name> \
  --fleet=<Fleet name>

# Example
python main.py session-recording \
  --bucket="session-recording-bucket" \
  --stack="AppStream-Demo-Stack" \
  --fleet="AppStream-Demo-Fleet"
```

### Generate the AppStream 2.0 S3 access logs

This function only works if you [enable S3 access log and create database and table in AWS Glue Data Catalog](https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/).

The preview of the report:

,bucket_name,key,operation,requestdatetime,UserName,FirstName,LastName
0,appstream2-36fb080bb8-ap-northeast-1-546614691476,user/userpool/7462108984f629db2ced1aeb2dc3e747e53a2e1c607059f72955ab864c724335/dummy.txt,REST.PUT.OBJECT,2022-09-01 02:37:30+00:00,demo@example.com,Demo,User

Please provide the required variables: database name and table name by specifying position arguments or flags.

There 2 variables are optional: `--datastart` and `--dateend`. Provide either both or at lease one of them. The value should be in ISO format like `YYYY-MM-DD hh:mm:ss +hh:mm`.

```bash
python main.py s3-accesslog \
  --database=<Database name> \
  --table=<Table name> \
  --datestart="<ISO data format>" \
  --dateend="<ISO data format>"

# Example
python main.py s3-accesslog \
  --database=s3_access_logs_db \
  --table=appstream2_logs \
  --datestart="2022-08-01 00:00:00 +0800" \
  --dateend="2022-09-01 00:00:00 +0800"
```
