![Buil and Test](https://github.com/jerry-enebeli/Blnk/actions/workflows/go.yml/badge.svg)
![Deploy To Docker](https://github.com/jerry-enebeli/Blnk/actions/workflows/docker-publish.yml/badge.svg)
![Linter](https://github.com/jerry-enebeli/Blnk/actions/workflows/lint.yml/badge.svg)

# Intro

Blnk is an open-source financial ledger server that enables you build financial products easily.

## Use Cases

- Banking
- Digital wallets
- Card processing
- Brokerage systems


# How Blnk works

# Ledgers
Ledgers are a common feature of financial systems, and are used to record and track transactions and balances for a particular entity. In a system such as Blnk, ledgers might be used to track the financial transactions and balances of customers, as well as other financial entities such as banks, businesses, or governments.

To create a new ledger in a system like Blnk, you would typically need to specify an identity for the ledger. This identity could be the customer's name or account number, or the name or identification number of another financial entity. The identity of the ledger is used to uniquely identify the ledger within the system, and to associate it with the appropriate transactions and balances.

Once a ledger has been created, all transactions and balances for that ledger can be recorded and tracked within the system. This might include recording and updating the balances for various accounts or financial assets, as well as tracking the flow of assets between accounts. By maintaining accurate and up-to-date ledgers, it is possible to track the financial activity of a particular entity and to ensure the integrity and accuracy of financial transactions.

# Balances
Balances are typically calculated for each  ledger, and represent the total amount of that asset that is available for use or transfer. Balances are typically updated every time a new transaction is recorded in the ledger, and can be used to track the flow of assets between accounts and to ensure that the ledger remains in balance.

### A balance consist of three sub balances.

| Name | Description |
| ------ | ------ |
| Credit Balance | Credit balance holds the sum of all credit transactions recorded|
| Debit Balance | Debit balance holds the sum of all debit transactions recorded  |
| Balance | The actual Balance is calculated by summing the Credit Balance and Debit Balance|


### Computing Balances
Balances are calculated for very new transaction entry to a ledger.

A ledger can have multiple balances, depending on the types of accounts and assets that it tracks. For example, a ledger might have separate balances for different currencies.

### Example

**Sample Transaction Entries**

| LedgerID | Currency | Amount | DRCR 
| ------ | ------ | ------ | ------ |
| 1 | USD| 100.00| CR
| 1 | USD| 50.00| CR 
| 1 | NGN| 50,000.00| CR 
| 1 | NGN| 1,000.00| CR 
| 1 | GHS| 1,000.00| CR
| 1 | USD| 50.00| DR 
| 1 | BTC| 1| DR


**Computed Balances**

| LedgerID | BalanceID | Currency | Credit Balance | Debit Balance | Balance
| ------ | ------ | ------ | ------ | ------ | ------ |
| 1 | 1 | USD  | 150.00 | 50.00 | 100.00
| 1 | 2 | NGN  | 51,000.00 | 0.00 | 51,000.00
| 1 | 3 | GHS  | 1,000.00 | 0.00 | 1,000.00
| 1 | 4 | BTC  | 1 | 0.00 | 1


### Balance Multiplier
Multipliers are used to convert balance to it's lowest currency denomination. Balances are multiplied by the multiplier and the result is stored as the balance value in the database.

**Before multiplier is applied**

| BalanceID | Currency | Credit Balance | Debit Balance | Balance | Multiplier
| ------ | ------ | ------ | ------ | ------ | ------ |
| 1 | USD  | 150.00 | 0.00 | 150.00 | 100
| 1 | BTC  | 1 | 0 | 1 | 100000000


**After multiplier is applied**

| BalanceID | Currency | Credit Balance | Debit Balance | Balance | Multiplier
| ------ | ------ | ------ | ------ | ------ | ------ |
| 1 | USD  | 15000 | 0 | 15000 | 100
| 2 | BTC  | 100000000 | 0 | 1 | 100000000


### Balance Properties

| Property | Description | Type |
| ------ | ------ | --- |
| id | Balance ID | string |
| ledger_id | The Ledger the balance belongs to | string |
| created | Timestamp of when the balance was created. | Time |
| currency | Balance currency | String
| balance | Derived from the summation of ```credit_balance``` and ```debit_balance``` | int64 |
| credit_balance | Credit Balance  | int64 |
| debit_balance |  Debit Balance | int64 |
| multiplier | Balance Multiplier | int64 |
| group | A group identifier | string |

# Transactions
Transactions record all ledger events. Transaction are recorded as either  ```Debit(DR)``` ```Credit(CR)```.


### Debit/Credit

```Debits``` and ```Credits``` are used to record all of the events that happen to a ledger, and to ensure that the ledger remains in balance. By using debits and credits, it is possible to track the movement of money between balances and to maintain an accurate record of financial transactions.

### Transaction Properties

| Property | Description | Type |
| ------ | ------ | --- |
| id | Transaction ID | string |
| amount | Transaction Amount| int64 |
| DRCR | Credit or Debit indicator| string |
| currency | Transaction currency | string
| ledger_id | The Ledger the transaction belongs to | string |
| balance_id | The balance the belongs to | string |
| status | The status of the transaction. example: ```Successful```, ```Pending```, ```Failed``` | string |
| reference | Unique Transaction referecence | string |
| group | A group identifier | string |
| description | Transaction description | string |
| meta_data | Custom metadata | object |

### Immutability
Transactions are immutable, this means that the records of the transaction cannot be altered or tampered with once they have been recorded. This is an important feature of transactions, as it ensures that the record of a transaction remains accurate and unchanged, even if the transaction itself is modified or reversed at a later time.

### Idempotency
Transactions are idempotent, "idempotent" means that the effect of a particular operation will be the same, no matter how many times it is performed. In other words, if a transaction is idempotent, then repeating the transaction multiple times will have the same result as performing it just once.

Idempotence is an important property of transactions, as it ensures that the outcome of the transaction is predictable and consistent, regardless of how many times it is performed. This can be particularly useful in situations where a transaction may be repeated due to network errors or other issues that may cause the transaction to fail.

**For example, consider a transaction that involves transferring money from one bank account to another. If the transaction is idempotent, then it will not matter how many times the transaction is repeated – the end result will always be the same. This helps to prevent unintended consequences, such as multiple transfers occurring or funds being mistakenly credited or debited multiple times.**

Blnk ensures Idempotency by leveraging ```reference```. Every transaction is expected to have a unique reference. Blnk ensures no two transactions are stored with the same reference. This helps to ensure that the outcome of the transaction is consistent, regardless of how many times the transaction is performed.


### Queuing Translations

Queuing plays a vital role in managing and optimizing transactions within Blnk. It enhances performance and ensures synchronization, allowing only one transaction to act on a balance at any given time. This section provides key insights into how Blnk leverages queues for efficient transaction processing.

#### Key Notes

1. **Partitioning for Similar Transactions**: Blnk employs a partitioning strategy where similar transactions belonging to the same balance are grouped together. This ensures that related transactions are processed sequentially, maintaining data consistency and reducing the likelihood of conflicts.

2. **Idempotency and Queue Transactions**: The idempotent nature of transactions, as discussed earlier, complements queue support seamlessly. By combining idempotency with queues, Blnk guarantees that even if a transaction is repeated, the outcome remains consistent. This is crucial in scenarios where network errors or other issues may cause a transaction to be reattempted.

#### Architecture Overview

To provide a visual representation of how queue support fits into the Blnk architecture, refer to the diagram below:

![Blnk Architecture](https://res.cloudinary.com/dp8bwjdvg/image/upload/v1697446181/lopw0lqnqwsyo5nul79z.png)

#### Supported Queue Systems

Blnk provides support for various queue systems to cater to different deployment and scalability requirements. As of the latest update, the following queue systems are supported:

1. **Confluent Kafka**: Blnk seamlessly integrates with Confluent Kafka, enabling efficient and reliable messaging for transaction processing.

2. **AWS SQS (Amazon Simple Queue Service)**: Support for AWS SQS is currently under development (⌛). This integration will extend Blnk's compatibility with cloud-based queue systems, allowing users to leverage AWS infrastructure for their transaction processing needs.

---

This expanded section should provide a more comprehensive understanding of how queue support enhances the Blnk system's performance and reliability while offering clear details on supported queue systems.


### Scheduling Transactions for Future Processing
Blnk offers a powerful feature that allows you to schedule transactions for future execution with ease. By including the "scheduled_for" parameter in your transaction payload, you can precisely specify when you want a transaction to take place. Here's a breakdown of how it works:

```json
{
    "amount": 10000,
    "tag": "Commission Earned",
    "reference": "ref",
    "drcr": "Credit",
    "currency": "NGN",
    "ledger_id": "ldg_3fa24854-211f-4248-9611-a7744aeaefcd",
    "balance_id": "bln_0abdbf91-d4b7-4917-bfdc-7fdf1680a3ca",
    "scheduled_for": "2023-10-08T19:48:00Z"
}
```


[//]: # (# Fault Tolerance)

[//]: # (Fault tolerance is a key aspect of any system design, as it helps ensure that the system can continue to function even in the event of failures or errors)

[//]: # ()
[//]: # (**By ```enabling fault tolerance in the config```, Blnk temporarily writes transactions to disk if they cannot be written to the database. This can help ensure that no transaction records are lost and that the system can continue to function even if the database experiences issues.**)

[//]: # (# Hooks To Transactions Agent 🤖)

[//]: # (An AI agent that takes in any transaction webhook payload from any payment provider and coverts them to blnk transaction payload. Write onece connect to any payment providers with ease)

[//]: # ()


# How To Install

## Option 1: Docker Image
```bash
$ docker run -v `pwd`/Blnk.json:/Blnk.json -p 4100:4100 jerryenebeli/blnk:latest
```

## Option 2: Building from source
To build Blnk from source code, you need:
* Go [version 1.16 or greater](https://golang.org/doc/install).

```bash
$ git clone https://github.com/jerry-enebeli/Blnk && cd Blnk
$ make build
```

# Get Started with Blnk
Blnk is a RESTFUL server. It exposes interaction with your Blnk server. The api exposes the following endpoints


## Create Config file ``blnk.json``

```json
{
  "port": "",
  "project_name": "",
  "default_currency": "NGN",
  "end_point_secret": "secret",
  "synchronization_method": "db lock | queue",
  "data_source": {
    "name": "POSTGRES",
    "dns": "postgres://postgres:@localhost:5432/blnk?sslmode=disable"
  },
  "confluent_kafka": {
    "server": "",
    "api_key": "",
    "secret_key": "",
    "queue_name": "transactions",
    "pull_wait_time": 100
  },
  "notification": {
    "slack": {
      "webhook_url": "https://webhook.test"
    },
    "webhook": {
      "url": "",
      "headers": {}
    }
  }
}
```

| Property | Description                                                                                                                              |
| ------ |------------------------------------------------------------------------------------------------------------------------------------------|
| port | Preferred port number for the server. default is  ```5100```                                                                             |
| project_name | Project Name.                                                                                                                            |
| default_currency | The default currency for new transaction entries. This would be used if a currency is not passed when creating a new transaction record. |
| enable_ft | Enable fault tolerance. default is false.                                                                                                |
| data_source | Database of your choice.                                                                                                                 |
| data_source.name | Name of preferred database. Blnk currently supports a couple of databases.                                                               |
| data_source.name | DNS of database                                                                                                                          |

### Supported Databases
| Data Base | Support |
| ------ | ------ |
| Postgres | ✅ |
