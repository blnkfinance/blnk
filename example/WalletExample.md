# Building a Wallet Solution With Blnk

## Step 1: Create ledger. Assume this is your main wallet project
Create ledger with an id. we can pass ```wirepay``` as the ledger id 

## Step 2: create customer wallet
Assuming you have collected your customer data and have it saved in your db. Creating a wallet for that customer is as easy as 
  - Get unique id for the newly created customer
  - Create Blnk Balance and us the customer id as the balance id
  - Repeat this step for as many customers as you have

## Step 3: Credit customer wallet after a topup is made
- Get Balance Id. In this case it should be similar to the customer id in your db
- Create a new transaction with the following
  - balance_id: jerry_id
  - amount: 1000
  - currency: NGN
  - reference: hello1
  - DRCR: Credit
  - tag: WALLET_TOP_UP


## Step 4: debit customer wallet when a transfer is made
- Get Balance Id. In this case it should be similar to the customer id in your db
- Create a new transaction with the following
    - balance_id: jerry_id
    - amount: 200
    - currency: NGN
    - reference: hello2
    - DRCR: Debit
    - tag: WALLET_TRANSFER


## Step 5 Get Balance by id
- Get Balance by id. this would return the following
  - id
  - created
  - credit_balance:  1000
  - debit_balance: 200
  - balance: 800 //customer actual balance
  - currency: NGN


## step 5 wallet to wallet transfer
- Create a new debit transaction on customer A balance
  - balance_id: jerry_id
  - amount: 200
  - currency: NGN
  - reference: hello3
  - DRCR: Debit
  - tag: WALLET_TO_WALLET_TRANSFER
- Create a new CREDIT transaction on customer B balance
    - balance_id: akin_id
    - amount: 200
    - currency: NGN
    - reference: hello4
    - DRCR: Credit
    - tag: WALLET_TO_WALLET_TRANSFER