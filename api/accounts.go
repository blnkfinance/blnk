/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package api

import (
	"net/http"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
)

// CreateAccount handles the creation of a new account.
// It binds the incoming JSON request body to a CreateAccount model, validates it,
// and creates the account if the input is valid.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding the JSON or validation fails.
// - 201 Created: If the account is successfully created.
// - 500 Internal Server Error: If there's an error in account creation.
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

// GetAccount retrieves an account by its ID.
// It uses the provided account ID and optional query parameters to fetch the account.
//
// Parameters:
// - c: The Gin context containing the request and response.
// - id: The unique identifier of the account to retrieve.
// - includes: Optional query parameters to include related data.
//
// Responses:
// - 400 Bad Request: If there's an error in fetching the account.
// - 200 OK: If the account is successfully retrieved.
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

// GetAllAccounts retrieves all accounts.
// It fetches a list of all accounts in the system.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in fetching the accounts.
// - 200 OK: If the accounts are successfully retrieved.
func (a Api) GetAllAccounts(c *gin.Context) {
	accounts, err := a.blnk.GetAllAccounts()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, accounts)
}

// generateMockAccount generates and returns a mock account for testing purposes.
// It provides a mock bank name and account number.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 200 OK: Returns a mock account with bank name and account number.
func (a Api) generateMockAccount(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"bank_name":      "Blnk Bank",
		"account_number": gofakeit.AchAccount(),
	})
}
