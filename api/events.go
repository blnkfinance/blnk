package api

import (
	"net/http"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/model"
)

func (a Api) CreateEvent(c *gin.Context) {
	var newEvent model2.CreateEvent
	if err := c.ShouldBindJSON(&newEvent); err != nil {
		return
	}

	err := newEvent.ValidateCreateEvent()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}
	resp, err := a.blnk.CreatEvent(newEvent.ToEvent())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) CreateEventMapper(c *gin.Context) {
	var newMapper model2.CreateEventMapper
	if err := c.ShouldBindJSON(&newMapper); err != nil {
		return
	}

	err := newMapper.ValidateCreateEventMapper()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateEventMapper(newMapper.ToEventMapper())
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
	var mapper model.EventMapper
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&mapper); err != nil {
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
