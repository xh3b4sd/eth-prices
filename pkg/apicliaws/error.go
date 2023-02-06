package apicliaws

import (
	"errors"
	"strings"

	"github.com/xh3b4sd/tracer"
)

var notFoundError = &tracer.Error{
	Kind: "notFoundError",
}

func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, notFoundError) || strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "Not Found")
}
