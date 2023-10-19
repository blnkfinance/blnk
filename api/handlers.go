package api

import (
	"fmt"
	"net/http"
	"reflect"

	"github.com/go-playground/validator/v10"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/pkg"

	"github.com/gin-gonic/gin"
)

type Api struct {
	blnk   *pkg.Blnk
	router *gin.Engine
}

func (a Api) Router() *gin.Engine {
	router := a.router
	router.POST("/ledger", a.CreateLedger)
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
	router.DELETE("/balance-monitor/:id", a.DeleteBalanceMonitor)

	return a.router
}

func NewAPI(blnk *pkg.Blnk) *Api {
	r := gin.Default()
	gin.SetMode(gin.DebugMode)
	return &Api{blnk: blnk, router: r}
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
