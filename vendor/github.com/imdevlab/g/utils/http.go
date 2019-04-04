package utils

import (
	"strings"

	"github.com/labstack/echo"
)

func FormValue(c echo.Context, key string) string {
	return strings.TrimSpace(c.FormValue(key))
}
