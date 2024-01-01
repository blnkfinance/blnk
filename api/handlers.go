package api

import (
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/pkg"

	"github.com/gin-gonic/gin"
)

type Api struct {
	blnk   *pkg.Blnk
	router *gin.Engine
}

type createCustomer struct {
	IdentityType string    `json:"identity_type"` // "individual" or "organization"
	FirstName    string    `json:"first_name"`
	LastName     string    `json:"last_name"`
	OtherNames   string    `json:"other_names"`
	Gender       string    `json:"gender"`
	DOB          time.Time `json:"dob"`
	EmailAddress string    `json:"email_address"`
	PhoneNumber  string    `json:"phone_number"`
	Nationality  string    `json:"nationality"`
	Name         string    `json:"name"`
	Category     string    `json:"category"`
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

	return a.router
}

func NewAPI(b *pkg.Blnk) *Api {
	r := gin.Default()
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
		var newCustomer blnk.Identity
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
		var newAccount blnk.Account
		if err := c.ShouldBind(&newAccount); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"errors": err})
			return
		}
		//get default ledger
		balance := blnk.Balance{
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

func ExtractValidationErrors(instance interface{}, errs validator.ValidationErrors) string {
	var errorMessages string

	for _, e := range errs {
		field, _ := reflect.TypeOf(instance).Elem().FieldByName(e.StructField())
		errorMessage := field.Tag.Get("error")

		if errorMessage == "" {
			errorMessage = e.Error() // Use the default error message from the validator
		}

		errorMessages = errorMessage
	}

	return errorMessages
}

func (a Api) CreateLedger(c *gin.Context) {
	var ledger blnk.Ledger
	if err := c.ShouldBindJSON(&ledger); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		errorMessages := ExtractValidationErrors(&ledger, validationErrors)
		c.JSON(http.StatusBadRequest, gin.H{"errors": errorMessages})
		return
	}
	resp, err := a.blnk.CreateLedger(ledger)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) CreateBalance(c *gin.Context) {
	var balance blnk.Balance
	if err := c.ShouldBindJSON(&balance); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		errorMessages := ExtractValidationErrors(&balance, validationErrors)
		c.JSON(http.StatusBadRequest, gin.H{"errors": errorMessages})
		return
	}
	resp, err := a.blnk.CreateBalance(balance)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) RecordTransaction(c *gin.Context) {
	var transaction blnk.Transaction
	if err := c.ShouldBindJSON(&transaction); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		errorMessages := ExtractValidationErrors(&transaction, validationErrors)
		c.JSON(http.StatusBadRequest, gin.H{"errors": errorMessages})
		return
	}
	resp, err := a.blnk.RecordTransaction(transaction)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) QueueTransaction(c *gin.Context) {
	var transaction blnk.Transaction
	if err := c.ShouldBindJSON(&transaction); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.blnk.QueueTransaction(transaction)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) RefundTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}
	resp, err := a.blnk.RefundTransaction(id)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetLedger(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetLedgerByID(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) GetAllLedgers(c *gin.Context) {
	resp, err := a.blnk.GetAllLedgers()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) GetBalance(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	// Extracting 'include' parameter from the query
	includes := c.QueryArray("include")

	fmt.Println(includes)
	resp, err := a.blnk.GetBalanceByID(id, includes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) GetTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetTransaction(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) GroupTransactionsByCurrency(c *gin.Context) {

	resp, err := a.blnk.GroupTransactionsByCurrency()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) CreateIdentity(c *gin.Context) {
	var identity blnk.Identity
	if err := c.ShouldBindJSON(&identity); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.blnk.CreateIdentity(identity)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetIdentity(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetIdentity(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) UpdateIdentity(c *gin.Context) {
	var identity blnk.Identity
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&identity); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	identity.IdentityID = id // Make sure the identity object has the right ID
	err := a.blnk.UpdateIdentity(&identity)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Identity updated successfully"})
}

func (a Api) DeleteIdentity(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	err := a.blnk.DeleteIdentity(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Identity deleted successfully"})
}

func (a Api) GetAllIdentities(c *gin.Context) {
	identities, err := a.blnk.GetAllIdentities()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, identities)
}

func (a Api) CreateEvent(c *gin.Context) {
	var event blnk.Event
	if err := c.ShouldBindJSON(&event); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		errorMessages := ExtractValidationErrors(&event, validationErrors)
		c.JSON(http.StatusBadRequest, gin.H{"errors": errorMessages})
		return
	}
	resp, err := a.blnk.CreatEvent(event)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) CreateEventMapper(c *gin.Context) {
	var mapper blnk.EventMapper
	if err := c.ShouldBindJSON(&mapper); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		errorMessages := ExtractValidationErrors(&mapper, validationErrors)
		c.JSON(http.StatusBadRequest, gin.H{"errors": errorMessages})
		return
	}
	resp, err := a.blnk.CreateEventMapper(mapper)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetAllEventMappers(c *gin.Context) {
	mappers, err := a.blnk.GetAllEventMappers()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, mappers)
}

func (a Api) GetEventMapperByID(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetEventMapperByID(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) UpdateEventMapper(c *gin.Context) {
	var mapper blnk.EventMapper
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&mapper); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		errorMessages := ExtractValidationErrors(&mapper, validationErrors)
		c.JSON(http.StatusBadRequest, gin.H{"errors": errorMessages})
		return
	}

	mapper.MapperID = id // Ensure the mapper object has the correct ID
	err := a.blnk.UpdateEventMapper(mapper)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "EventMapper updated successfully"})
}

func (a Api) DeleteEventMapper(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	err := a.blnk.DeleteEventMapperByID(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "EventMapper deleted successfully"})
}

func (a Api) CreateBalanceMonitor(c *gin.Context) {
	var monitor blnk.BalanceMonitor
	if err := c.ShouldBindJSON(&monitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.blnk.CreateMonitor(monitor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetBalanceMonitor(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetMonitorByID(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) GetAllBalanceMonitors(c *gin.Context) {
	monitors, err := a.blnk.GetAllMonitors()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, monitors)
}

func (a Api) GetBalanceMonitorsByBalanceID(c *gin.Context) {
	balanceID, passed := c.Params.Get("balance_id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "balance_id is required. pass balance_id in the route /:balance_id"})
		return
	}

	monitors, err := a.blnk.GetMonitorByID(balanceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, monitors)
}

func (a Api) UpdateBalanceMonitor(c *gin.Context) {
	var monitor blnk.BalanceMonitor
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&monitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	monitor.MonitorID = id
	err := a.blnk.UpdateMonitor(&monitor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "BalanceMonitor updated successfully"})
}

func (a Api) DeleteBalanceMonitor(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	err := a.blnk.DeleteMonitor(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "BalanceMonitor deleted successfully"})
}

func (a Api) CreateAccount(c *gin.Context) {
	var account blnk.Account
	if err := c.ShouldBindJSON(&account); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.blnk.CreateAccount(account)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetAccount(c *gin.Context) {
	id := c.Param("id")
	account, err := a.blnk.GetAccount(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, account)
}

func (a Api) UpdateAccount(c *gin.Context) {
	id := c.Param("id")
	var account blnk.Account
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

func (a Api) GetCustomerList(c *gin.Context) {
	resp, err := a.blnk.GetAllIdentities()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.HTML(200, "customers-list.html", gin.H{
		"customers": resp,
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
