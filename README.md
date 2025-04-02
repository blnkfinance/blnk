![Blnk logo](https://res.cloudinary.com/dmxizylxw/image/upload/v1724847576/blnk_github_logo_eyy2lf.png)

<br/>

## Status at a Glance

![Build and Test Status](https://github.com/blnkfinance/blnk/actions/workflows/go.yml/badge.svg)
![Deploy to Docker Status](https://github.com/blnkfinance/blnk/actions/workflows/docker-publish.yml/badge.svg)
![Linter Status](https://github.com/blnkfinance/blnk/actions/workflows/lint.yml/badge.svg)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)

<br/>

## Open-Source Financial Ledger for Developers

Blnk Finance is a developer-first toolkit designed for developers who want to **ship fintech products fast** without compromising compliance and correctness.

This toolkit consists of:

* **Ledger:** Our open-source double-entry ledger for managing balances and recording any transaction workflow. Features include balance monitoring, balance snapshots, historical balances, inflight transactions, scheduling and overdrafts, bulk transactions, and so much more.

* **Reconciliation:** Automatically match external records like bank statements to your internal ledger with custom matching rules and reconciliation strategies.

* **Identity Management:** Easily create & manage identities with PII tokenization features and the ability to link to balances and transactions.

Here are some fintech use cases for Blnk:

1. [Wallet Management](https://docs.blnkfinance.com/tutorials/quick-start/wallet-management)
2. [Deposits & Withdrawals](https://docs.blnkfinance.com/tutorials/digital-banking/deposits-withdrawals)
3. [Order Exchange](https://docs.blnkfinance.com/tutorials/crypto/order-exchange)
4. [Lending](https://docs.blnkfinance.com/tutorials/digital-banking/lending)
5. [Loyalty Points System](https://docs.blnkfinance.com/tutorials/quick-start/loyalty-points)
6. [Savings Application](https://docs.blnkfinance.com/tutorials/quick-start/savings-application)
7. [Escrow Application](https://docs.blnkfinance.com/tutorials/quick-start/escrow-payments)

### Love what we're building?

* Star our repo to help more developers discover Blnk → [Give us a star](https://github.com/blnkfinance/blnk)
* Join our community on Discord → [Accept Discord invite](https://discord.gg/7WNv94zPpx)
* Check out Blnk Cloud → [Visit website](https://www.blnkfinance.com)

<br/>

## Quick Start

The quickest way to get acquainted with Blnk is by installing it and diving into [our documentation](https://docs.blnkfinance.com) for a hands-on experience.

Here are a few things to try:

1. [Create your first ledger, balance, and transaction](https://docs.blnkfinance.com/home/install#3-create-your-first-ledger)
2. [Hold an inflight transaction and commit/void](https://docs.blnkfinance.com/transactions/inflight)
3. [Record a transaction from one balance to 2 or more balances](https://docs.blnkfinance.com/transactions/multiple-destinations)
4. [Record multiple transactions at once](https://docs.blnkfinance.com/transactions/bulk-transactions)
5. [Record backdated transactions](https://docs.blnkfinance.com/transactions/backdated-transactions)
6. [Create an identity](https://docs.blnkfinance.com/identities/introduction)
7. [Link an identity to a balance](https://docs.blnkfinance.com/identities/link-balances)

<br/>

## Installation

To install Blnk, make sure you have [Docker Compose](https://docs.docker.com/compose/) installed and running on your machine.

1. Clone the repository into your machine:
   
   ```
   git clone https://github.com/blnkledger/Blnk && cd Blnk
   ```
   
2. Create a configuration file, `blnk.json`, copy the following configuration and save:

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
       "port": "5001"
     }
   }
   ```

3. Start your Blnk server with Docker Compose:

   ```
   docker compose up
   ```

<br/>

## Contributions

Contributions and feedback are welcome and encouraged. Join our [Discord community](https://discord.gg/7WNv94zPpx) to do so, and connect with other developers from around the world.

<br/>

## License

This project uses the [Apache License 2.0](LICENSE.md).
