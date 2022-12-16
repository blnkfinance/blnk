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


| Property | Description |
| ------ | ------ |
| port | preferred port number for the server. default is  ```4300``` |
| project_name | Name of your project. |
| default_currency | Default currency for new transaction entries. This would be used if a currency is not passed |
| enable_wal | Enable write-ahead log. default is false. |
| data_source | Database of your choice.  |
| data_source.name | Name of preferred database. Saifu currently supports a couple of databases. |
| data_source.name | DNS of database|

| Data Base | Support |
| ------ | ------ |
| Postgres | ✅ |
| MYSQL | ✅ |
| MongoDB | ✅ |
| Redis | ✅ |
