package check

import (
	"github.com/antekresic/grs/domain"
	"github.com/go-playground/validator"
)

//Entry is an implementation of an entry validator using the go-playground validator
type Entry struct {
	Validator *validator.Validate
}

//Validate is used to validate entries
func (e Entry) Validate(entry domain.Entry) error {
	return e.Validator.Struct(entry)
}
