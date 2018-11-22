package mock

import (
	"time"

	"github.com/antekresic/grs/domain"
)

//TestClock implements a fake clock for testing purposes
type TestClock struct {
	Time time.Time
}

//Now provides the time set in TestClock
func (r TestClock) Now() time.Time {
	return r.Time
}

//TestRepo is a mock of the domain.EntryRepository used for testing purposes
type TestRepo struct {
	AddEntryEntry           domain.Entry
	AddEntryReturnError     error
	GetEntriesLastID        string
	GetEntriesReturnEntries []domain.Entry
	GetEntriesReturnLastID  string
	GetEntriesReturnError   error
	StoreCursorCursor       domain.StreamCursor
	StoreCursorReturnError  error
	GetCursorsReturnCursors []domain.StreamCursor
	GetCursorsReturnError   error
	StealCursorOldCursor    domain.StreamCursor
	StealCursorNewName      string
	StealCursorReturnError  error
}

//AddEntry records the input params and returns specified results
func (t *TestRepo) AddEntry(e domain.Entry) error {
	t.AddEntryEntry = e
	return t.AddEntryReturnError
}

//GetEntries records the input params and returns specified results
func (t *TestRepo) GetEntries(lastID string) (entries []domain.Entry, newLastID string, err error) {
	t.GetEntriesLastID = lastID
	return t.GetEntriesReturnEntries, t.GetEntriesReturnLastID, t.GetEntriesReturnError
}

//StoreCursor records the input params and returns specified results
func (t *TestRepo) StoreCursor(c domain.StreamCursor) error {
	t.StoreCursorCursor = c
	return t.StoreCursorReturnError
}

//GetCursors records the input params and returns specified results
func (t *TestRepo) GetCursors() (cursors []domain.StreamCursor, err error) {
	return t.GetCursorsReturnCursors, t.GetCursorsReturnError
}

//StealCursor records the input params and returns specified results
func (t *TestRepo) StealCursor(oldCursor domain.StreamCursor, newName string) error {
	t.StealCursorOldCursor, t.StealCursorNewName = oldCursor, newName
	return t.StealCursorReturnError
}
