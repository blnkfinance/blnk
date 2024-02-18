package api

import (
	"fmt"
	"log"
	"net/http"

	"github.com/jerry-enebeli/blnk/config"
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
	router.POST("/ledger", a.CreateLedger)
	router.GET("/ledger", a.GetAllLedgers)
	router.POST("/balance", a.CreateBalance)
	router.POST("/transaction", a.RecordTransaction)
	router.POST("/transaction-queue", a.QueueTransaction)
	router.POST("/refund-transaction/:id", a.RefundTransaction)
	router.GET("/ledger/:id", a.GetLedger)
	router.GET("/balance/:id", a.GetBalance)
	router.GET("/transaction/:id", a.GetTransaction)
	router.GET("/transaction/group/currency", a.GroupTransactionsByCurrency)
	// Identity routes
	router.POST("/identity", a.CreateIdentity)
	router.GET("/identity/:id", a.GetIdentity)
	router.PUT("/identity/:id", a.UpdateIdentity)
	router.DELETE("/identity/:id", a.DeleteIdentity)
	router.GET("/identities", a.GetAllIdentities)
	router.POST("/event", a.CreateEvent)
	router.POST("/event-mapper", a.CreateEventMapper)
	router.GET("/event-mapper/:id", a.GetEventMapperByID)
	router.GET("/event-mappers", a.GetAllEventMappers)
	router.PUT("/event-mapper/:id", a.UpdateEventMapper)
	router.DELETE("/event-mapper/:id", a.DeleteEventMapper)
	router.POST("/balance-monitor", a.CreateBalanceMonitor)
	router.GET("/balance-monitor/:id", a.GetBalanceMonitor)
	router.GET("/balance-monitors", a.GetAllBalanceMonitors)
	router.GET("/balance-monitors/balance/:balance_id", a.GetBalanceMonitorsByBalanceID)
	router.PUT("/balance-monitor/:id", a.UpdateBalanceMonitor)
	router.POST("/account", a.CreateAccount)
	router.GET("/account/:id", a.GetAccount)
	router.PUT("/account/:id", a.UpdateAccount)
	router.DELETE("/account/:id", a.DeleteAccount)
	router.GET("/accounts", a.GetAllAccounts)
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

	cfg, err := config.Fetch()
	if err != nil {
		log.Panicln(err)
	}

	if *cfg.UiEnabled {
		r.LoadHTMLGlob("ui/*.html")
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
	}

	gin.SetMode(gin.DebugMode)

	return &Api{blnk: b, router: r}
}
