## STEP TO RECREATE LOCALLY
```
cd quis
pip install -r requirements.txt
mkdir rawdata
mkdir parquet
```
Once the rawdata folder is made, drag and drop your CSV file and name it to quis_data.csv

```
python3 pipeline.py
streamlit run dashboard.py
```
The Data Quality / Slack Alert will not work until you set up OAuth
## STEP TO SETUP SLACK OAUTH

- Follow this guide to setup a bot and fetch a bot token: https://docs.slack.dev/quickstart/
- Copy and paste this token(start with xoxb) to alerter.py, also you can set which slack channel to receive information from this bot
- Remember to invite this bot to your slack channel
- Restart the streamlit app
