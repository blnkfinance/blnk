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

---
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


---
## Balance Monitoring
Balance monitoring is a crucial component in the fintech world. With transactions being processed every second, it's essential to ensure that balances remain accurate, updated, and within expected thresholds. BLNK offers an efficient way to monitor these balances in real-time, ensuring that financial operations run smoothly and anomalies are detected promptly.

### Why Monitor Balances?

In the fintech space, monitoring balances is vital for several reasons:

1. **Fraud Detection**: Unusual balance changes can be an early indication of fraudulent activities. Monitoring can trigger alerts for suspicious transactions, enabling timely intervention.
2. **Regulatory Compliance**: Many financial regulations require institutions to maintain specific balance thresholds. Real-time monitoring ensures compliance with these regulations.
3. **Customer Notifications**: Customers can be notified in real-time if their account balance goes below a certain threshold, helping them manage their finances better.
4. **Operational Efficiency**: Instantly knowing when a balance reaches a certain threshold can trigger automatic actions, such as transferring funds between accounts or purchasing assets.

### Creating a Balance Monitor with BLNK

BLNK simplifies the process of setting up balance monitors. With a straightforward API, you can set conditions that, when met, trigger specific actions, such as sending a webhook event.

Here's how you can create a balance monitor:

```json
{
    "balance_id": "bln_c1750613-b4b0-4cde-9793-459165a8715f",
    "condition": {
        "field": "debit_balance",
        "operator": ">",
        "value": 100000
    }
}
```

In the example above, a monitor is set up for the balance with ID `bln_c1750613-b4b0-4cde-9793-459165a8715f`. The condition checks if the `debit_balance` exceeds 100,000. If this condition is met, BLNK will trigger the predefined action, such as sending a webhook event.

#### Supported Fields
BLNK supports monitoring on the following fields:

- `balance`: The total balance.
- `credit_balance`: The total credit balance.
- `debit_balance`: The total debit balance.

### Use Case: Real-time Alert for High-Value Transactions

Imagine a fintech platform that processes high-value transactions for institutional clients. These clients need to be immediately informed if a large debit occurs in their accounts to ensure that it's an authorized transaction.

Using BLNK's balance monitoring feature, the platform can set up monitors for each institutional client's account. If a debit exceeds a certain threshold, say $1,000,000, BLNK will instantly send a webhook event. This can be integrated into the platform's notification system to alert the client via email, SMS, or a mobile app notification.

---
# Identity
The Identity feature in Blnk provides a robust mechanism to attach a unique identity to each balance, ensuring every financial activity is traceable and structured. Whether it's individual customers or larger organizations, every transaction can be attributed to a specific entity, enhancing transparency and accountability.

## Linking Identity to Balance in Blnk

In Blnk, each balance can be associated with a unique identity, ensuring clear attribution of financial activities. This association enhances the traceability and accountability of transactions and balances within the system.

### How to Link an Identity to a Balance

1. **Create an Identity**: Before you can link an identity to a balance, the identity (be it an individual or an organization) must be created and stored in Blnk. Upon creation, every identity is assigned a unique `identity_id`.

2. **Create a Balance with an Identity**: When creating a new balance, you can link it to an existing identity by passing the `identity_id` in the balance creation payload.

Here's a sample payload to create a balance and link it to an identity:

```json
{
    "ledger_id": "ldg_db5eabf0-4152-47cf-8353-d1729a491962",
    "identity_id": "idt_0501db5c-baf9-4be1-a931-f4bae7f3a41d",
    "currency": "NGN"
}
```

In this payload:

- `ledger_id`: Represents the unique ID of the ledger where the balance is to be created.
- `identity_id`: The unique ID of the identity you want to link to the balance.
- `currency`: The currency of the balance.

Once the balance is created with the above payload, it will be intrinsically linked to the specified identity through the `identity_id`. This linkage ensures that all transactions and activities associated with this balance can be traced back to the specified identity, adding an extra layer of transparency to the financial operations in Blnk.
## Identity Attributes

| Property      | Description                                                      | Type                                        |
| ------------- | ---------------------------------------------------------------- |---------------------------------------------|
| IdentityType  | Distinguishes if the identity is an individual or organization   | string (`"individual"` or `"organization"`) |
| Individual    | Contains details if the identity type is an individual           | `Individual` attributes (see below)         |
| Organization  | Details if the identity type is an organization                  | `Organization` attributes (see below)       |
| Street        | Street address of the identity                                   | string                                      |
| Country       | Country where the identity resides                               | string                                      |
| State         | State of residence of the identity                               | string                                      |
| PostCode      | Postal code related to the identity                              | string                                      |
| City          | City of residence of the identity                                | string                                      |
| MetaData      | Custom metadata linked to the identity                           | Object                                      |

## Individual Attributes
| Property       | Description                                                      | Type   |
| -------------- | ---------------------------------------------------------------- |--------|
| FirstName      | First name of the individual                                     | string |
| LastName       | Last name of the individual                                      | string |
| OtherNames     | Any other names linked to the individual                          | string |
| Gender         | Gender of the individual                                          | string |
| DOB            | Date of birth of the individual                                  | Date   |
| EmailAddress   | Email ID of the individual                                        | string |
| PhoneNumber    | Contact number of the individual                                  | string |
| Nationality    | Nationality of the individual                                     | string |

## Organization Attributes
| Property   | Description                                      | Type   |
| ---------- | ------------------------------------------------ | ------ |
| Name       | Name of the organization                         | string |
| Category   | Category or type of the organization (e.g., "Bank", "Retail", "Tech") | string |

---
# Transactions
Transactions record all ledger events. Transaction are recorded as either  ```Debit(DR)``` ```Credit(CR)```.


### Debit/Credit

```Debits``` and ```Credits``` are used to record all of the events that happen to a ledger, and to ensure that the ledger remains in balance. By using debits and credits, it is possible to track the movement of money between balances and to maintain an accurate record of financial transactions.

### Transaction Properties

| Property | Description                                                                          | Type |
| ------ |--------------------------------------------------------------------------------------| --- |
| id | Transaction ID                                                                       | string |
| amount | Transaction Amount                                                                   | int64 |
| DRCR | Credit or Debit indicator                                                            | string |
| currency | Transaction currency                                                                 | string
| ledger_id | The Ledger the transaction belongs to                                                | string |
| balance_id | The balance the belongs to                                                           | string |
| status | The status of the transaction. example: ```APPLIED```, ```QUEUED```, ```SCHEDULED``` | string |
| reference | Unique Transaction referecence                                                       | string |
| group | A group identifier                                                                   | string |
| description | Transaction description                                                              | string |
| meta_data | Custom metadata                                                                      | object |

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

1. **Confluent Kafka** ✅: Blnk seamlessly integrates with Confluent Kafka, enabling efficient and reliable messaging for transaction processing.

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

Certainly! Let's simplify and provide a more developer-centric explanation:

---
# Fraud Detection
Blnk's fraud detection module is designed to compute a "fraud score" for financial transactions, providing a risk assessment metric. This documentation section will delve deep into the algorithm behind this essential feature.

### **Constants Overview**

- **maxChangeFrequency**: Maximum number of transactions observed in a day for any account.
- **maxTransactionAmount**: Highest transaction value recorded across all accounts.
- **maxBalance**: Peak account balance observed in the system.
- **maxCreditBalance**: Maximum credit balance ever recorded.
- **maxDebitBalance**: Largest debit balance observed.

### **Normalization Process**

Normalization standardizes varying financial parameters to a scale of 0-1, where 0 represents no or minimal risk and 1 signifies the highest possible risk. The function:

```pseudo
function normalize(value, min=0, max) {
    return (value - min) / (max - min)
}
```
is employed for this purpose. This ensures each parameter contributes proportionally to the final fraud score.

### **Fraud Score Computation**

1. **Input Normalization**: Parameters like transaction amount, balance, etc., are normalized using the constants.
2. **Weight Assignment**: Each normalized parameter gets a weight, defining its influence on the final score.
3. **Score Calculation**: The fraud score is the sum of the product of each parameter and its weight, ensuring it remains between 0 (minimal risk) and 1 (maximum risk).

Absolutely. Here's the revised documentation using `risk_tolerance_threshold`.


## **Applying Blnk's Fraud Detection Module**

### **Using the `risk_tolerance_threshold` Field**:

1. **risk_tolerance_threshold**:
    - Type: Float (Range: 0 to 1)
    - Description: Represents the maximum acceptable risk level for the transaction. A score of `0` indicates no risk, while a score of `1` denotes maximum risk. By setting this value, you instruct the system to evaluate the transaction's risk level and compare it against your threshold.
    - Example: If you set `risk_tolerance_threshold` to `0.7`, any transaction with a computed fraud score above 0.7 will be flagged or denied, while transactions with a score of 0.7 or below will be processed.

### **Steps to Apply Fraud Detection**:

1. **Initiate a Transaction**:
    - When initiating a financial transaction, include all necessary fields in the JSON payload, such as `amount`, `tag`, `reference`, `drcr`, `currency`, and `balance_id`.

2. **Include `risk_tolerance_threshold`**:
    - Decide on the maximum risk you're willing to accept for this transaction.
    - Add the `risk_tolerance_threshold` field to your transaction payload and set it to your chosen value.

3. **Process the Transaction**:
    - Send the transaction payload to Blnk's system.
    - Blnk's fraud detection module will compute the transaction's fraud score.
    - If the computed score is below or equal to your specified `risk_tolerance_threshold`, the transaction will proceed. Otherwise, it will be flagged or denied based on the system's configured response to high-risk transactions.

4. **Review and Respond**:
    - For flagged transactions, review the details and decide whether to approve or deny them. You can adjust the `risk_tolerance_threshold` for future transactions based on your comfort level and observed transaction patterns.

**Note**: Adjusting the `risk_tolerance_threshold` allows you to fine-tune the sensitivity of the fraud detection system. A lower value makes the system more stringent, flagging more transactions, while a higher value makes it more lenient.

---

## Events & Mappers

The fintech domain is vast, with diverse systems speaking different "data languages." Integrating them can be a challenge. Blnk tackles this with **Events** and **Mappers**, making integrations smoother and more consistent.
### Events

**Events** in Blnk are external financial activities that need to be documented or processed. Think of them as real-time notifications about transactions, verifications, or any other significant activity.These could be anything from a successful card transaction to a transfer verification.

### Mappers

While events notify Blnk about an activity, **Mappers** ensure Blnk understands this information. They act as "translators," converting data from various external systems into a standardized format that Blnk can process.

### Quick Example:

Let's say you're connecting Blnk to different payment processors like Bloc and Stripe. Each sends transaction details differently.

Instead of tweaking Blnk for each system, you use **Mappers**. For a `transaction.new` event from Bloc with fields like `data.amount` and `data.reference`, a mapper translates this into Blnk's language.

### Code Samples:

**Creating a Mapper for Bloc**:
```json
{
    "name": "Bloc transaction webhook mapping",
    "mapping_instruction": {
        "amount": "{data.amount}",
        "reference": "{data.reference}",
        "currency": "{data.currency}"
    }
}
```

**Creating an Event using the created Bloc Mapper**:
```json
{
    "mapper_id": "map_3db6f05a-8f5a-43e4-a91c-b5a6c5e7d5a1",
    "drcr": "Credit",
    "balance_id": "bln_c1750613-b4b0-4cde-9793-459165a8715f",
    "data": {
        "event": "transaction.new",
        "data": {
            "amount": 30000,
            "currency": "NGN",
            "reference": "1jhbs3ozmen0k7y5e333feeme3rrr33ewwedffdew",
            "source": "Collection Account",
            "status": "successful"
        }
    }
}
```

**Generates and records transaction for processing**:
```json
{
  "id": "txn_9da2b9bc-a0f4-4832-bb2a-5a493eedb5b3",
  "tag": "",
  "reference": "1jhbs3ozmen0k7y5e333feeme3rrr33ewwedffdew",
  "amount": 30000,
  "currency": "NGN",
  "drcr": "Credit",
  "status": "QUEUED",
  "ledger_id": "ldg_db5eabf0-4152-47cf-8353-d1729a491962",
  "balance_id": "bln_c1750613-b4b0-4cde-9793-459165a8715f",
  "credit_balance_before": 246000,
  "debit_balance_before": 0,
  "credit_balance_after": 276000,
  "debit_balance_after": 0,
  "balance_before": 246000,
  "balance_after": 276000,
  "created_at": "2023-10-19T07:59:22.052869+01:00",
  "scheduled_for": "0001-01-01T00:00:00Z"
}
```

# How To Install

## Option 1: Docker Image
```bash
$ docker run -v `pwd`/blnk.json:/blnk.json -p 5001:5001 jerryenebeli/blnk:latest
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
  "port": "5001",
  "project_name": "My Fintech",
  "end_point_secret": "secret",
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
