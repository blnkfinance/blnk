# Intro

Saifu is a financial ledger server that enables you build financial products easily.

# Use Cases

- Banking
- Digital wallets

# How Saifu works

## Ledgers
ledgers are the entry point of saifu. They are mostly tied to an entity(user). Ledgers are connected to Balances. Every entry to a ledger either create a new balance or recompute and existing balance. 

## Balances
Balances are update to date reflection of all entries in a ledger. Balances are precalculated for very new entry to a ledger

### Creating a Balance
Balances are auto created when a new transaction entry is recorded for a ledger. A ledger can have multiple balances. Balances are composed of ```Currency``` and ```LedgerID```

### Type of Balances
Balances in Saifu are made of of three major balance.

| Name | Description |
| ------ | ------ |
| Credit Balance | Credit balance hold the sum of all credit transactions recorded |
| Debit Balance | Debit balance hold the sum of all debit transactions recorded  |
| Balance | This is calculated by summing the Credit Balance and Debit Balance |


### Computing Balances
Balances are precalculated for very new transaction entry to a ledger

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


**Computed Balances**

| LedgerID | BalanceID | Currency | Credit Balance | Debit Balance | Balance
| ------ | ------ | ------ | ------ | ------ | ------ |
| 1 | 1 | USD  | 150.00 | 50.00 | 100.00
| 1 | 2 | NGN  | 51,000.00 | 0.00 | 150.00
| 1 | 3 | GHS  | 1,000.00 | 0.00 | 150.00



## Transactions


## Write Ahead Log

## Config file

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
| port | preferred port number for the server. default is  ```4300``` |
| project_name | Name of your project. |
| default_currency | Default currency for new transaction entries. This would be used if a currency is not passed |
| enable_wal | Enable write-ahead log. default is false. |
| data_source | Database of your choice.  |
| data_source.name | Name of preferred database. Saifu currently supports a couple of databases. |
| data_source.name | DNS of database|

## Supported Databases
| Data Base | Support |
| ------ | ------ |
| Postgres | ✅ |
| MYSQL | ✅ |
| MongoDB | ✅ |
| Redis | ✅ |


## How To Install

### Option 1: Docker Image
```bash
$ docker run \
	-p 5005:5005 \
	--name saifu \
    --network=host \
	-v `pwd`/saifu.json:/saifu.json \
	docker.cloudsmith.io/saifu/saifu:latest
```

### Option 2: Building from source
To build saifu from source code, you need:
* Go [version 1.16 or greater](https://golang.org/doc/install).

```bash
$ git clone https://github.com/jerry-enebeli/saifu && cd saifu
$ make build
```

## Get Started with Saiffu
Saifu is a RESTFUL server. It exposes interact with your saifu server. The API exposes the following endpoints

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