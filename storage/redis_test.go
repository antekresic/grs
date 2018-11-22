package storage

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/antekresic/grs/domain"
	"github.com/antekresic/grs/mock"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestStorage(mock RedisClient) domain.EntryRepository {
	return &RedisRepository{
		Client: mock,
	}

}

func TestAddEntry(t *testing.T) {
	t.Run("Add valid entry", func(t *testing.T) {
		mockClient := &mock.TestRedisClient{
			XAddReturnStringCmd: redis.NewStringResult("result", nil),
		}

		entry := domain.Entry{
			ObjectID: 42,
			Action:   "create",
		}

		content, err := json.Marshal(entry)

		require.Nil(t, err, "Error marshaling entry")

		values := map[string]interface{}{entryField: content}

		storage := getTestStorage(mockClient)

		err = storage.AddEntry(entry)

		assert.Nil(t, err, "Error is not nil")
		assert.Equal(t, mockClient.XAddArgs.Stream, streamName, "Stream name not correct")
		assert.Equal(t, mockClient.XAddArgs.Values, values, "Values not correct")
	})

	t.Run("Xadd error", func(t *testing.T) {
		mockClient := &mock.TestRedisClient{
			XAddReturnStringCmd: redis.NewStringResult("", errors.New("some error")),
		}

		entry := domain.Entry{
			ObjectID: 42,
			Action:   "create",
		}

		content, err := json.Marshal(entry)

		require.Nil(t, err, "Error marshaling entry")

		values := map[string]interface{}{entryField: content}

		storage := getTestStorage(mockClient)

		err = storage.AddEntry(entry)

		assert.NotNil(t, err, "Error is not nil")
		assert.Equal(t, mockClient.XAddArgs.Stream, streamName, "Stream name not correct")
		assert.Equal(t, mockClient.XAddArgs.Values, values, "Values not correct")
	})
}

func TestGetCursors(t *testing.T) {
	t.Run("Get cursors", func(t *testing.T) {
		results := []string{
			"", "name", "123",
			"1", "otherName", "42",
		}
		mockClient := &mock.TestRedisClient{
			SortReturnStringSliceCmd: redis.NewStringSliceResult(results, nil),
		}

		storage := getTestStorage(mockClient)

		cursors, err := storage.GetCursors()

		assert.Equal(t, len(cursors), len(results)/3, "Cursor count doesn't match the results")
		assert.Nil(t, err, "Error is not nil")

		firstCursor := cursors[0]
		firstResult := results[0:3]

		assert.Equal(t, firstCursor.Name, firstResult[1], "First cursor Name not correct")
		assert.Equal(t, firstCursor.LastID, firstResult[2], "First cursor LastID not correct")
		assert.Equal(t, firstCursor.HasHeart, firstResult[0] != "", "First cursor HasHeart not correct")

		secondCursor := cursors[1]
		secondResult := results[3:]

		assert.Equal(t, secondCursor.Name, secondResult[1], "Second cursor Name not correct")
		assert.Equal(t, secondCursor.LastID, secondResult[2], "Second cursor LastID not correct")
		assert.Equal(t, secondCursor.HasHeart, secondResult[0] != "", "Second cursor HasHeart not correct")

	})

	t.Run("Sort returns redis.Nil", func(t *testing.T) {
		mockClient := &mock.TestRedisClient{
			SortReturnStringSliceCmd: redis.NewStringSliceResult(nil, redis.Nil),
		}

		storage := getTestStorage(mockClient)

		cursors, err := storage.GetCursors()

		assert.Nil(t, err, "Error is not nil")
		assert.Empty(t, cursors, "Cursors are not empty")
	})

	t.Run("Sort returns some error", func(t *testing.T) {
		mockClient := &mock.TestRedisClient{
			SortReturnStringSliceCmd: redis.NewStringSliceResult(nil, errors.New("some error")),
		}

		storage := getTestStorage(mockClient)

		cursors, err := storage.GetCursors()

		assert.NotNil(t, err, "Error is nil")
		assert.Empty(t, cursors, "Cursors are not empty")
	})

	t.Run("Sort returns results that make no sense (less than 3 strings)", func(t *testing.T) {
		results := []string{
			"", "name",
		}
		mockClient := &mock.TestRedisClient{
			SortReturnStringSliceCmd: redis.NewStringSliceResult(results, nil),
		}

		storage := getTestStorage(mockClient)

		cursors, err := storage.GetCursors()

		assert.Nil(t, err, "Error is not nil")
		assert.Empty(t, cursors, "Cursors are not empty")
	})
}
