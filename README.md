# Intro

Saifu is a financial ledger platform that allows users to build financial products such as bank accounts and digital wallets.

# Use Cases

- Bank accounts
- Digital wallets

# Features

- Connect to any database
- Create ledger
- Create balance
- Record transaction
- Get ledgers
- Get balances
- Scheduled new record
- Write-ahead logs
- Reconciliation

# Get Started

## Create Config file

```json
{
  "port": "4100",
  "project_name": "Eyowo",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}
```


| Plugin | README |
| ------ | ------ |
| Dropbox | [plugins/dropbox/README.md][PlDb] |
| GitHub | [plugins/github/README.md][PlGh] |
| Google Drive | [plugins/googledrive/README.md][PlGd] |
| OneDrive | [plugins/onedrive/README.md][PlOd] |
| Medium | [plugins/medium/README.md][PlMe] |
| Google Analytics | [plugins/googleanalytics/README.md][PlGa] |
