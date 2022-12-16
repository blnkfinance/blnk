# Saifu
Blnk wallet is a simple to use wallet service. it provides out the box functionalities for a wallet have

#features
- connect to various databases
- create a digital wallet for any currency
- wallet to wallet transfer
- ability to debit a wallet
- ability to credit a wallet
- scheduled credit
- scheduled debit
- currency swap
- wallet to wallet transfer
- set exchange rate for currencies
- Balances
  - main balance
  - holding balance
- Fees
  - credit fee
  - debit fee
  - revenue wallet:  where all fees are stored
- Reporting/analytics
  - total wallets
  - total transactions
  - sum balances

#config file
* project name
* define how to secure
  * jwt - pass secret
  * basic auth - pass username and password
* Database
  * select database
  * Supported databases
    * Mongo
    * postgres
    * mysql
    * Redis
  * pass connection requirements
* Errors
  * sentry connector
  * other error reporting connectors
* Monitoring
  * new-relic
* channels
  * rest api
  * ussd
* Notification
  * webhooks
  * slack
  * emails

# Deployments
This provides easy ways to deploy/run the application
* Docker

# CLi
* create project
  * pass configs# saifu
