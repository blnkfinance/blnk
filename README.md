# Intro

Saifu is a financial ledger tool that allows users to build financial products such as bank accounts and digital wallets. Saifu

# Use Cases

- Banking
- Digital wallets

# How it works

## Ledgers


## Balances


## Transactions


## Transaction Tags

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

### Databases
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

## Accessing Saifu
Saifu is a RESTful server. It exposes interact with your saifu server. The API exposes the following endpoints

###Create ledger ```POST```
```/ledgers```

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

###Get Ledgers ```GET```
```/ledgers```

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

###Get Ledger Balances ```GET```
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

###Record Transaction ```POST```
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


###Get Recorded Transactions ```GET```
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