package httpx

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type APIErrorResponse struct {
	Error any `json:"error"`
}

func BadRequest(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, APIErrorResponse{Error: err.Error()})
}

func NotFound(c *gin.Context, message string) {
	c.JSON(http.StatusNotFound, APIErrorResponse{Error: message})
}

func InternalError(c *gin.Context) {
	c.JSON(http.StatusInternalServerError, APIErrorResponse{Error: "internal server error"})
}

func OK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, data)
}

func OKEmpty(c *gin.Context) {
	c.Status(http.StatusOK)
}

func Created(c *gin.Context, data any) {
	c.JSON(http.StatusCreated, data)
}

func NoContent(c *gin.Context) {
	c.Status(http.StatusOK)
}
