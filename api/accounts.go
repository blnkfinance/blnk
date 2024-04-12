package api

import (
	"net/http"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
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

	includes := c.QueryArray("include")

	account, err := a.blnk.GetAccount(id, includes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, account)
}

func (a Api) GetAllAccounts(c *gin.Context) {
	accounts, err := a.blnk.GetAllAccounts()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, accounts)
}

func (a Api) generateMockAccount(c *gin.Context) {
	c.JSON(200, gin.H{
		"bank_name":      "Blnk Bank",
		"account_number": gofakeit.AchAccount(),
	})
}
