![Blnk logo](https://res.cloudinary.com/dmxizylxw/image/upload/v1724847576/blnk_github_logo_eyy2lf.png)

## Status at a Glance
![Build and Test Status](https://github.com/blnkfinance/blnk/actions/workflows/go.yml/badge.svg)
![Deploy to Docker Status](https://github.com/blnkfinance/blnk/actions/workflows/docker-publish.yml/badge.svg)
![Linter Status](https://github.com/blnkfinance/blnk/actions/workflows/lint.yml/badge.svg)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)

## Open-source fintech ledger for the internet

Blnk Ledger is an open-source fintech database for building fintech products to standard and at scale. We designed Blnk to help developers do three things well:

1. Accurately record transactions in their system.
2. Correctly manage complex flow of funds and transaction data.
3. Reliably manage the size of your transactions as your product scales.

## About Blnk

Building fintech starts off simple, and gets quickly difficult & complex the more time you spend with it. Blnk offers specialized tools to help developers today manage and navigate this complexity correctly for their respective organizations.

Every fintech product is made up of transactions — happening concurrently, moving all the time from one place to another. Sometimes, they will interact with parties outside of your application like cards, deposits, bank accounts, etc; sometimes, they happen within your application between user balances or to/fro your organization accounts.

With Blnk, you get a full-service starting point for building, managing and operating money movement and store of value in a reliable, secure and scaleable way. Here are some use-cases of the Blnk Ledger:

- [Multi-currency wallets](https://docs.blnkfinance.com/ledger/examples/multicurrency-wallets)
- [Virtual card issuance](https://docs.blnkfinance.com/ledger/examples/virtual-cards-issuance)
- [Loyalty points system](https://docs.blnkfinance.com/ledger/examples/customer-loyalty-points-system)
- [Expense management system](https://docs.blnkfinance.com/ledger/examples/expense-management)
- [Escrow application](https://docs.blnkfinance.com/ledger/examples/escrow-application)
- [Savings application](https://docs.blnkfinance.com/ledger/examples/savings-application)

## Quick Links
- [Visit the Blnk Website](https://blnkfinance.com)
- [Our developer documentation](https://docs.blnkfinance.com)

## Quickstart

The fastest way to get acquainted with Blnk is by [installing Blnk](#installation). If you'd like to read more information about how Blnk works, you can get started here:

- [Welcome to Blnk](https://docs.blnkfinance.com/intro/welcome)
- [Understanding Double Entry](https://docs.blnkfinance.com/ledger/guide/double-entry-101)
- [Create ledgers and ledger balances](https://docs.blnkfinance.com/ledger/tutorial/create-a-ledger)
- [Record a transaction](https://docs.blnkfinance.com/ledger/tutorial/record-a-transaction)

## Installation

To install Blnk, make sure you have [Docker](https://www.docker.com/) and [Compose](https://docs.docker.com/compose/) installed and running on your machine.

To get started with Blnk, first clone the repository into your machine:
```
git clone https://github.com/blnkledger/Blnk && cd Blnk
```
and create a configuration file, `blnk.json`:
```
touch blnk.json
```
then copy and save the following configuration:
```json
{
  "project_name": "Blnk",
  "data_source": {
    "dns": "postgres://postgres:password@postgres:5432/blnk?sslmode=disable"
  },
  "redis": {
    "dns": "redis:6379"
  },
  "server": {
    "domain": "blnk.io",
    "ssl": false,
    "ssl_email": "jerryenebeli@gmail.com",
    "port": "5001"
  },
  "notification": {
    "slack": {
      "webhook_url": "https://hooks.slack.com"
    }
  }
}
```
then start your Blnk server with Docker Compose:
```
docker compose up
```

## Contributions

Contributions and feedback are welcome and encouraged. Join our [Discord community](https://discord.gg/7WNv94zPpx) to do so, and connect with other developers from around the world.

## License

This project uses the [Apache License 2.0](LICENSE.md).
