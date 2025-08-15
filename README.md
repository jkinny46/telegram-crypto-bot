Crypto bot pulling data from crypto_fundraising channel into a google sheet to be cleaned, transformed, and queryed. 

Run: (file_directory) / python3 tg_backfill_range.py --from YYYY-MM-DD --to YYYY-MM-DD in terminal to run script.

Free setup using Gemini free tier to backfill / update based on date range. 

Requires your API token from telegram and Gemini, and Gsheets integration to function. 

Service account template below for needed keys to add to files.  

{
  "type": "service_account",
  "project_id": 
  "private_key_id":
  "private_key": "-----BEGIN PRIVATE KEY-----\
  "client_email": 
  "client_id": 
  "auth_uri": 
  "token_uri": 
  "auth_provider_x509_cert_url":
  "client_x509_cert_url": 
  "universe_domain": "googleapis.com"
}
