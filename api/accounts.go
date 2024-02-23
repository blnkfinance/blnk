package api

import (
	"net/http"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/model"
)

func (a Api) CreateAccount(c *gin.Context) {
	var newAccount model2.CreateAccount
	if err := c.ShouldBindJSON(&newAccount); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := newAccount.ValidateCreateAccount()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateAccount(newAccount.ToAccount())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetAccount(c *gin.Context) {
	id := c.Param("id")

	// Extracting 'include' parameter from the query
	includes := c.QueryArray("include")

	account, err := a.blnk.GetAccount(id, includes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, account)
}

func (a Api) UpdateAccount(c *gin.Context) {
	id := c.Param("id")
	var account model.Account
	if err := c.ShouldBindJSON(&account); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	account.AccountID = id
	err := a.blnk.UpdateAccount(&account)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Account updated successfully"})
}

func (a Api) DeleteAccount(c *gin.Context) {
	id := c.Param("id")
	err := a.blnk.DeleteAccount(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Account deleted successfully"})
}

func (a Api) GetAllAccounts(c *gin.Context) {
	accounts, err := a.blnk.GetAllAccounts()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, accounts)
}

func (a Api) GetIdentityList(c *gin.Context) {
	resp, err := a.blnk.GetAllIdentities()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.HTML(200, "identity-list.html", gin.H{
		"identities": resp,
	})
}

func (a Api) GetAccountList(c *gin.Context) {
	resp, err := a.blnk.GetAllAccounts()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.HTML(200, "accounts-list.html", gin.H{
		"accounts": resp,
	})
}

func (a Api) GetLedgerList(c *gin.Context) {
	resp, err := a.blnk.GetAllLedgers()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.HTML(200, "ledger-list.html", gin.H{
		"ledgers": resp,
	})
}

func (a Api) GetLedgerPage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Ledger Page",
		"template": "ledger.html",
	})
}

func (a Api) GetHomePage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Home Page",
		"template": "home.html",
	})
}

func (a Api) GetBalancePage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Balance Page",
		"template": "balance.html",
	})
}

func (a Api) GetAccountPage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Account Page",
		"template": "account.html",
	})
}

func (a Api) GetIdentityPage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Identity Page",
		"template": "identity.html",
	})
}

func (a Api) GetTransactionPage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Transaction Page",
		"template": "transactions.html",
	})
}
func (a Api) GetAuditPage(c *gin.Context) {
	c.HTML(200, "layout.html", gin.H{
		"Title":    "Audit Page",
		"template": "audit.html",
	})
}

func (a Api) GetBalanceList(c *gin.Context) {
	resp, err := a.blnk.GetAllBalances()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.HTML(200, "balance-list.html", gin.H{
		"balances": resp,
	})
}

func (a Api) TransactionList(c *gin.Context) {
	resp, err := a.blnk.GetAllTransactions()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.HTML(200, "transactions-list.html", gin.H{
		"transactions": resp,
	})
}

func (a Api) AccountDetails(c *gin.Context) {
	resp, err := a.blnk.GetAllTransactions()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.HTML(200, "account-details.html", gin.H{
		"transactions": resp,
	})
}

func (a Api) generateMockAccount(c *gin.Context) {
	c.JSON(200, gin.H{
		"bank_name":      "Blnk Bank",
		"account_number": gofakeit.AchAccount(),
	})
}
