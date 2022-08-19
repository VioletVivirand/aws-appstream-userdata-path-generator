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
