# Intro

Saifu is a financial ledger server that enables you build financial products easily.

## Use Cases

- Banking
- Digital wallets
- Card processing
- Brokerage systems


# How Saifu works

## Ledgers
Ledgers are the entry point of Saifu. Every transaction entry and balances are tied to a ledger. Ledgers are created by passing an identity.Ledger Identity can be of a customer or any financial entity in your system.

### Ledger Identity

## Balances
Balances show the balance actual balance(s) of a ledger. Balances are pre-computed on every new entry to a ledger.

### Type of Balances

| Name | Description |
| ------ | ------ |
| Credit Balance | Credit balance holds the sum of all credit transactions recorded|
| Debit Balance | Debit balance holds the sum of all debit transactions recorded  |
| Balance | The actual Balance is calculated by summing the Credit Balance and Debit Balance|


### Computing Balances
Balances are calculated for very new transaction entry to a ledger. A ledger can have multiple balances. Balances are composed of ```Currency``` and ```LedgerID```

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
| 1 | 2 | NGN  | 51,000.00 | 0.00 | 150.00
| 1 | 3 | GHS  | 1,000.00 | 0.00 | 150.00
| 1 | 4 | BTC  | 1 | 0.00 | 1


### Balance Multiplier
Multipliers are used to convert balance to it's lowest currency denomination. Balances are multiplied by the multiplier and the result is stored as the balance value in the db```balance * multiplier```

**Before multiplier is applied**

| BalanceID | Currency | Credit Balance | Debit Balance | Balance | Multiplier
| ------ | ------ | ------ | ------ | ------ | ------ |
| 1 | USD  | 150.00 | 0.00 | 150.00 | 100
| 1 | BTC  | 1 | 1 | 1 | 100000000


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


## Transactions
Transactions record all events that happen to a ledger. Transaction fall into two major categories. ```Debit(DR)``` ```Credit(CR)```

### Transaction Properties

| Property | Description | Type |
| ------ | ------ | --- |
| id | Balance ID | string |
| amount | The Ledger the balance belongs to | int64 |
| currency | Balance currency | String
| ledger_id | The Ledger the balance belongs to | string |
| status | The Ledger the balance belongs to | string |
| reference | The Ledger the balance belongs to | string |
| tag | The Ledger the balance belongs to | string |
| group | The Ledger the balance belongs to | string |
| description | The Ledger the balance belongs to | string |
| meta_data | The Ledger the balance belongs to | Object |

### Immutability
Transactions are immutable, this means that the records of the transaction cannot be altered or tampered with once they have been recorded. This is an important feature of transactions, as it ensures that the record of a transaction remains accurate and unchanged, even if the transaction itself is modified or reversed at a later time.

### Idempotency
Transactions are idempotent, "idempotent" means that the effect of a particular operation will be the same, no matter how many times it is performed. In other words, if a transaction is idempotent, then repeating the transaction multiple times will have the same result as performing it just once.

Idempotence is an important property of transactions, as it ensures that the outcome of the transaction is predictable and consistent, regardless of how many times it is performed. This can be particularly useful in situations where a transaction may be repeated due to network errors or other issues that may cause the transaction to fail.

For example, consider a transaction that involves transferring money from one bank account to another. If the transaction is idempotent, then it will not matter how many times the transaction is repeated – the end result will always be the same. This helps to prevent unintended consequences, such as multiple transfers occurring or funds being mistakenly credited or debited multiple times.

Saifu ensures Idempotency by leveraging ```referece``` Every transaction is expected to have a unique reference. Saifu ensures no two transactions are stored with the same reference. This helps to ensure that the outcome of the transaction is consistent, regardless of how many times the transaction is performed.

### Grouping



### Tags


## Fault Tolerance


# How To Install

## Option 1: Docker Image
```bash
$ docker run \
	-p 5005:5005 \
	--name saifu \
    --network=host \
	-v `pwd`/saifu.json:/saifu.json \
	docker.cloudsmith.io/saifu/saifu:latest
```

## Option 2: Building from source
To build saifu from source code, you need:
* Go [version 1.16 or greater](https://golang.org/doc/install).

```bash
$ git clone https://github.com/jerry-enebeli/saifu && cd saifu
$ make build
```

# Get Started with Saiffu
Saifu is a RESTFUL server. It exposes interaction with your Saifu server. The API exposes the following endpoints


## Create Config file ``saifu.json``

```json
{
  "port": "4100",
  "project_name": "MyWallet",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}
```

| Property | Description |
| ------ | ------ |
| port | Preferred port number for the server. default is  ```4300``` |
| project_name | Project Name. |
| default_currency |  The default currency for new transaction entries. This would be used if a currency is not passed when creating a new transaction record. |
| enable_wal | Enable write-ahead log. default is false. |
| data_source | Database of your choice.  |
| data_source.name | Name of preferred database. Saifu currently supports a couple of databases. |
| data_source.name | DNS of database|


### Supported Databases
| Data Base | Support |
| ------ | ------ |
| Postgres | ✅ |
| MYSQL | ✅ |
| MongoDB | ✅ |
| Redis | ✅ |


## Endpoints

### Create ledger ```POST```
```/ledgers```

**Request**
```json
{
  "id": "cu_ghjoipeysnsfu24"
}
```

**Response**
```json
{
  "id": "cu_ghjoipeysnsfu24"
}
```

### Get Ledgers ```GET```
```/ledgers```

**Response**
```json
[{
  "port": "4100",
  "project_name": "MyWallet",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}]
```

### Get Ledger Balances ```GET```
```/ledgers/balances/{ID}```

**Response**
```json
{
  "port": "4100",
  "project_name": "MyWallet",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}
```

### Record Transaction ```POST```
```/transactions```

**Request**
```json
{
  "port": "4100",
  "project_name": "MyWallet",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}
```

**Response**
```json
{
  "port": "4100",
  "project_name": "MyWallet",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}
```


### Get Recorded Transactions ```GET```
```/transactions```

**Response**
```json
{
  "port": "4100",
  "project_name": "MyWallet",
  "default_currency": "NGN",
  "data_source": {
    "name": "MONGO",
    "dns":""
  }
}
```