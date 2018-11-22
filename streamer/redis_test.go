package streamer

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/antekresic/grs/domain"
	"github.com/antekresic/grs/mock"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func getTestStreamer(mockRepo domain.EntryRepository, clock Clock) RedisStreamer {
	return RedisStreamer{
		Repo:  mockRepo,
		Clock: clock,
	}
}

func TestGetEntries(t *testing.T) {

	t.Run("Return entries from stream", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			GetEntriesReturnEntries: []domain.Entry{
				domain.Entry{
					ID:         "myEntryID",
					ObjectID:   123,
					ObjectType: 3,
					Action:     "create",
					Meta:       "JSON",
				},
			},
			GetEntriesReturnLastID: "myLastID",
			GetEntriesReturnError:  nil,

			GetCursorsReturnCursors: []domain.StreamCursor{
				domain.StreamCursor{
					Name:     "myCursor",
					LastID:   "myID",
					HasHeart: true,
				},
				domain.StreamCursor{
					Name:     "foo",
					LastID:   "bar",
					HasHeart: false,
				},
			},
			GetCursorsReturnError: nil,

			StealCursorReturnError: nil,
		}

		cursorForStealCursor := mockRepo.GetCursorsReturnCursors[1]

		streamer := getTestStreamer(mockRepo, nil)
		entries, err := streamer.GetEntries()

		assert.NotEmpty(t, streamer.cursor.Name, "Cursor name is empty")
		assert.Equal(
			t,
			streamer.cursor.LastID,
			mockRepo.GetEntriesReturnLastID,
			"GetEntries returned wrong LastID",
		)

		assert.Equal(
			t,
			cursorForStealCursor.Name,
			mockRepo.StealCursorOldCursor.Name,
			"StealCursor old cursor Name wrong",
		)
		assert.Equal(
			t,
			cursorForStealCursor.LastID,
			mockRepo.StealCursorOldCursor.LastID,
			"StealCursor old cursor LastID wrong",
		)
		assert.Equal(
			t,
			int64(ConsumerTimeout),
			mockRepo.StealCursorOldCursor.HeartTimeout,
			"StealCursor old cursor HeartTimeout wrong",
		)

		assert.Equal(
			t,
			entries,
			mockRepo.GetEntriesReturnEntries,
			"GetEntries returned wrong entries",
		)
		assert.Nil(t, err, "GetEntries returned non-nil error")

	})

	t.Run("GetCursors returns an error", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			GetCursorsReturnError: errors.New("some error"),
		}

		streamer := getTestStreamer(mockRepo, nil)

		entries, err := streamer.GetEntries()

		assert.Nil(t, entries, "GetEntries returned non-nil entries")
		assert.NotNil(t, err, "GetEntries returned a nil err")

	})

	t.Run("GetEntries returns an error", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			GetCursorsReturnCursors: nil,
			GetCursorsReturnError:   nil,
			GetEntriesReturnError:   errors.New("some error"),
		}

		streamer := getTestStreamer(mockRepo, nil)

		entries, err := streamer.GetEntries()

		assert.Nil(t, entries, "GetEntries returned non-nil entries")
		assert.NotNil(t, err, "GetEntries returned a nil err")

	})

	t.Run("StealCursors returns an error", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			GetCursorsReturnCursors: []domain.StreamCursor{
				domain.StreamCursor{
					Name:     "foo",
					LastID:   "bar",
					HasHeart: false,
				},
			},
			GetCursorsReturnError:  nil,
			StealCursorReturnError: errors.New("some error"),
		}
		cursorForStealCursor := mockRepo.GetCursorsReturnCursors[0]

		streamer := getTestStreamer(mockRepo, nil)

		entries, err := streamer.GetEntries()

		assert.Equal(
			t,
			cursorForStealCursor.Name,
			mockRepo.StealCursorOldCursor.Name,
			"StealCursor old cursor Name wrong",
		)
		assert.Equal(
			t,
			cursorForStealCursor.LastID,
			mockRepo.StealCursorOldCursor.LastID,
			"StealCursor old cursor LastID wrong",
		)
		assert.Equal(
			t,
			int64(ConsumerTimeout),
			mockRepo.StealCursorOldCursor.HeartTimeout,
			"StealCursor old cursor HeartTimeout wrong",
		)

		assert.Nil(t, entries, "GetEntries returned non-nil entries")
		assert.NotNil(t, err, "GetEntries returned a nil err")
	})

	t.Run("StealCursors returns an redis.TxFailedErr error", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			GetCursorsReturnCursors: []domain.StreamCursor{
				domain.StreamCursor{
					Name:     "foo",
					LastID:   "bar",
					HasHeart: false,
				},
			},
			GetCursorsReturnError:  nil,
			StealCursorReturnError: redis.TxFailedErr,
		}
		cursorForStealCursor := mockRepo.GetCursorsReturnCursors[0]

		streamer := getTestStreamer(mockRepo, nil)

		entries, err := streamer.GetEntries()

		assert.Equal(
			t,
			cursorForStealCursor.Name,
			mockRepo.StealCursorOldCursor.Name,
			"StealCursor old cursor Name wrong",
		)
		assert.Equal(
			t,
			cursorForStealCursor.LastID,
			mockRepo.StealCursorOldCursor.LastID,
			"StealCursor old cursor LastID wrong",
		)
		assert.Equal(
			t,
			int64(ConsumerTimeout),
			mockRepo.StealCursorOldCursor.HeartTimeout,
			"StealCursor old cursor HeartTimeout wrong",
		)

		assert.Nil(t, entries, "GetEntries returned non-nil entries")
		assert.Nil(t, err, "GetEntries returned a nil err")

		assert.NotEmpty(t, streamer.cursor.Name, "Cursor name is empty")
		assert.Equal(t, "$", streamer.cursor.LastID, "Cursor LastID is not $")
	})
}

func TestMarkEntryProcessed(t *testing.T) {

	t.Run("Store non-overdue ID", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			StoreCursorReturnError: nil,
		}
		clock := mock.TestClock{
			Time: time.Now(),
		}
		ID := fmt.Sprintf("%d-0", clock.Time.Unix()*1000)
		streamerName := "someName"

		streamer := getTestStreamer(mockRepo, clock)
		streamer.cursor.Name = streamerName

		err := streamer.MarkEntryProcessed(ID)

		assert.Equal(
			t,
			streamerName,
			mockRepo.StoreCursorCursor.Name,
			"Did not store the correct name",
		)
		assert.Equal(
			t,
			ID,
			mockRepo.StoreCursorCursor.LastID,
			"Did not store the correct ID",
		)
		assert.Equal(
			t,
			int64(ConsumerTimeout),
			mockRepo.StoreCursorCursor.HeartTimeout,
			"Did not store the correct HeartTimeout",
		)

		assert.Nil(t, err, "Error is not nil")

	})

	t.Run("Store overdue ID", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			StoreCursorReturnError: nil,
		}
		clock := mock.TestClock{
			Time: time.Now().Add(2 * ConsumerTimeout),
		}
		ID := fmt.Sprintf("%d-0", time.Now().Unix()*1000)
		streamerName := "someName"

		streamer := getTestStreamer(mockRepo, clock)
		streamer.cursor.Name = streamerName

		err := streamer.MarkEntryProcessed(ID)

		assert.Equal(
			t,
			streamerName,
			mockRepo.StoreCursorCursor.Name,
			"Did not store the correct name",
		)
		assert.Equal(
			t,
			ID,
			mockRepo.StoreCursorCursor.LastID,
			"Did not store the correct ID",
		)
		assert.Equal(
			t,
			int64(ConsumerTimeout),
			mockRepo.StoreCursorCursor.HeartTimeout,
			"Did not store the correct HeartTimeout",
		)

		assert.Nil(t, err, "Error is not nil")

	})

	t.Run("Store invalid ID", func(t *testing.T) {
		mockRepo := &mock.TestRepo{
			StoreCursorReturnError: nil,
		}
		clock := mock.TestClock{
			Time: time.Now().Add(ConsumerTimeout * -2),
		}
		ID := "invalidID"
		streamerName := "someName"

		streamer := getTestStreamer(mockRepo, clock)
		streamer.cursor.Name = streamerName

		err := streamer.MarkEntryProcessed(ID)

		assert.Equal(
			t,
			streamerName,
			mockRepo.StoreCursorCursor.Name,
			"Did not store the correct name",
		)
		assert.Equal(
			t,
			ID,
			mockRepo.StoreCursorCursor.LastID,
			"Did not store the correct ID",
		)
		assert.Equal(
			t,
			int64(ConsumerTimeout),
			mockRepo.StoreCursorCursor.HeartTimeout,
			"Did not store the correct HeartTimeout",
		)

		assert.Nil(t, err, "Error is not nil")

	})
}
