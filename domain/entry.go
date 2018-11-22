package domain

//Entry represents an entry in the event stream
type Entry struct {
	ID         string `json:"-"`
	ObjectID   int    `json:"object_id" validate:"required"`
	ObjectType int    `json:"object_type" validate:"required"`
	Action     string `json:"action" validate:"oneof=create update delete"`
	Meta       string `json:"meta" validate:"eq=JSON"`
}

//StreamCursor holds information about stream consumer last location
type StreamCursor struct {
	Name         string
	LastID       string
	HeartTimeout int64
	HasHeart     bool
}

//EntryRepository is an interface for persisting entries
type EntryRepository interface {
	AddEntry(Entry) error
	GetEntries(lastID string) (entries []Entry, newLastID string, err error)
	StoreCursor(StreamCursor) error
	GetCursors() (cursors []StreamCursor, err error)
	StealCursor(oldCursor StreamCursor, newName string) error
}

//EntryStreamer streams entries and marks them as processed
type EntryStreamer interface {
	GetEntries() ([]Entry, error)
	MarkEntryProcessed(ID string) error
}

//EntryValidator is an interface for validating entries
type EntryValidator interface {
	Validate(Entry) error
}

//EntryConsumer consumes the entry
type EntryConsumer interface {
	Consume(Entry) error
}
