package api

import (
	"fmt"
	"net/http"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk"
)

type Api struct {
	blnk   *blnk.Blnk
	router *gin.Engine
}

func (a Api) Router() *gin.Engine {
	router := a.router
	router.POST("/ledgers", a.CreateLedger)
	router.GET("/ledgers/:id", a.GetLedger)
	router.GET("/ledgers", a.GetAllLedgers)

	router.POST("/balances", a.CreateBalance)
	router.GET("/balances/:id", a.GetBalance)

	router.POST("/balance-monitors", a.CreateBalanceMonitor)
	router.GET("/balance-monitors/:id", a.GetBalanceMonitor)
	router.GET("/balance-monitors", a.GetAllBalanceMonitors)
	router.GET("/balance-monitors/balances/:balance_id", a.GetBalanceMonitorsByBalanceID)
	router.PUT("/balance-monitors/:id", a.UpdateBalanceMonitor)

	router.POST("/transactions", a.QueueTransaction)
	router.POST("/refund-transaction/:id", a.RefundTransaction)
	router.GET("/transactions/:id", a.GetTransaction)

	router.POST("/identities", a.CreateIdentity)
	router.GET("/identities/:id", a.GetIdentity)
	router.PUT("/identities/:id", a.UpdateIdentity)
	router.GET("/identities", a.GetAllIdentities)

	router.POST("/accounts", a.CreateAccount)
	router.GET("/accounts/:id", a.GetAccount)
	router.PUT("/accounts/:id", a.UpdateAccount)
	router.DELETE("/accounts/:id", a.DeleteAccount)
	router.GET("/accounts", a.GetAllAccounts)

	router.POST("/events", a.CreateEvent)
	router.POST("/event-mappers", a.CreateEventMapper)
	router.GET("/event-mappers/:id", a.GetEventMapperByID)
	router.GET("/event-mappers", a.GetAllEventMappers)
	router.PUT("/event-mappers/:id", a.UpdateEventMapper)
	router.DELETE("/event-mappers/:id", a.DeleteEventMapper)

	//ui routes
	router.GET("/customer-list", a.GetCustomerList)
	router.GET("/account-list", a.GetAccountList)
	router.GET("/transaction-list", a.TransactionList)
	router.GET("/accounts-details/:account_id", a.AccountDetails)
	router.GET("/mocked-account", a.generateMockAccount)

	return a.router
}

func NewAPI(b *blnk.Blnk) *Api {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	//r.LoadHTMLGlob("ui/*.html")
	r.Static("/static", "ui")
	r.GET("/", func(c *gin.Context) {
		// Render the "index.html" template
		c.HTML(200, "index.html", gin.H{
			"title": "Home Page",
		})
	})
	r.GET("/create-ledger", func(c *gin.Context) {
		c.HTML(200, "create-ledger.html", gin.H{
			"title": "Home Page",
		})
	})
	r.GET("/create-customer", func(c *gin.Context) {
		c.HTML(200, "create-customer.html", gin.H{
			"title": "Home Page",
		})
	})
	r.GET("/success", func(c *gin.Context) {
		c.HTML(200, "successful.html", gin.H{
			"message":   "Customer created",
			"next_page": "/",
		})
	})

	r.POST("/webhook", func(c *gin.Context) {
		var payload map[string]interface{}
		err := c.Bind(&payload)
		if err != nil {
			return
		}
		fmt.Println(payload)
	})

	r.POST("/create-customer", func(c *gin.Context) {
		var newCustomer model.Identity
		if err := c.ShouldBind(&newCustomer); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"errors": err})
			return
		}
		_, err := b.CreateIdentity(newCustomer)
		if err != nil {
			fmt.Println(err)
			return
		}
		c.HTML(200, "successful.html", gin.H{
			"message":   "Customer created",
			"next_page": "/",
		})
	})

	r.POST("/create-account", func(c *gin.Context) {
		var newAccount model.Account
		if err := c.ShouldBind(&newAccount); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"errors": err})
			return
		}
		//get default ledger
		balance := model.Balance{
			Currency:   "NGN",
			LedgerID:   "ldg_43e18b22-458f-47ce-9c60-a0704b3741fa",
			IdentityID: newAccount.IdentityID,
		}

		newBalance, err := b.CreateBalance(balance)
		if err != nil {
			fmt.Println(err)
			return
		}
		newAccount.BalanceID = newBalance.BalanceID
		newAccount.LedgerID = newBalance.LedgerID
		_, err = b.CreateAccount(newAccount)
		if err != nil {
			fmt.Println(err)
			return
		}
		c.HTML(200, "successful.html", gin.H{
			"message":   "Account created",
			"next_page": "/",
		})
	})

	r.GET("/customer-select", func(c *gin.Context) {
		customers, err := b.GetAllIdentities()
		if err != nil {
			return
		}
		c.HTML(200, "customer-select.html", gin.H{
			"customers": customers,
		})
	})

	gin.SetMode(gin.DebugMode)
	return &Api{blnk: b, router: r}
}
