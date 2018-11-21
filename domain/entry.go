package domain

//Entry represents an entry in the event stream
type Entry struct {
	ID         string `json:"-"`
	ObjectID   int    `json:"object_id" validate:"required"`
	ObjectType int    `json:"object_type" validate:"required"`
	Action     string `json:"action" validate:"oneof=create update delete"`
	Meta       string `json:"meta" validate:"eq=json"`
}

//EntryRepository is an interface for persisting entries
type EntryRepository interface {
	AddEntry(Entry) error
	GetEntries() ([]Entry, error)
	Ack(ID string) error
}

//EntryValidator is an interface for validating entries
type EntryValidator interface {
	Validate(Entry) error
}

//EntryConsumer consumes the entry
type EntryConsumer interface {
	Consume(Entry) error
}
