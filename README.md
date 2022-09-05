# AWS AppStream User Data Path Generator

| 🚧 Disclaimer: This project is still under construction 🚧

## Usage

To export the home folder path of AppStream 2.0 users:

```bash
python main.py home-folder
```

It will export the result to `report_homefolder.csv`.

Export the session recording path or AppStream 2.0 users:

```bash
python main.py session-recording --bucket=<Bucket name> --stack=<Stack name> --fleet=<Fleet name>
```

It will export the result to `report_sessionrecording.csv`.

Export the AppStream 2.0 S3 access logs:

```bash
python main.py s3-accesslog \
  --database=s3_access_logs_db \
  --table=appstream2_logs \
  --datestart="2022-08-01 00:00:00 +0800" \
  --dateend="2022-09-03 00:00:00 +0800"
```

It will export the result to `report_s3log.csv`