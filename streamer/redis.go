package streamer

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/antekresic/grs/domain"
	"github.com/go-redis/redis"
	uuid "github.com/satori/go.uuid"
)

const (
	//ConsumerTimeout is the time alloted for processing an entry
	ConsumerTimeout time.Duration = 5 * time.Second
)

//Clock provides the current time
type Clock interface {
	Now() time.Time
}

//RealClock implements the Clock interface using the time package
type RealClock struct{}

//Now provides the current time
func (r RealClock) Now() time.Time {
	return time.Now()
}

//RedisStreamer manages the entries stream from Redis
type RedisStreamer struct {
	Repo   domain.EntryRepository
	Clock  Clock
	cursor domain.StreamCursor
}

//MarkEntryProcessed stores info about the streamer and last ID processed.
func (r RedisStreamer) MarkEntryProcessed(ID string) error {
	//check if ID is over time limit and report it back
	if r.isAckOverdue(ID) {
		log.Printf("Consumer %s finished processing entry %s after timeout\n", r.cursor.Name, ID)
	}

	return r.Repo.StoreCursor(domain.StreamCursor{
		Name:         r.cursor.Name,
		LastID:       ID,
		HeartTimeout: int64(ConsumerTimeout),
	})
}

//GetEntries fetches events from Redis Stream.
func (r *RedisStreamer) GetEntries() ([]domain.Entry, error) {
	if r.cursor.Name == "" {
		err := r.identify()

		if err != nil {
			return nil, fmt.Errorf("GetEntries: %s", err.Error())
		}
	}

	entries, lastID, err := r.Repo.GetEntries(r.cursor.LastID)

	if err != nil {
		return nil, fmt.Errorf("GetEntries: %s", err.Error())
	}

	if lastID != "" {
		r.cursor.LastID = lastID
	}

	return entries, nil
}

//identify trys to assume the name and position of the consumer which has stopped.
func (r *RedisStreamer) identify() error {
	cursors, err := r.Repo.GetCursors()

	if err != nil {
		return fmt.Errorf("identify: %s", err.Error())
	}

	for _, cursor := range cursors {

		//Skip consumer which is still alive.
		if cursor.HasHeart {
			continue
		}

		r.cursor.Name = getUniqueName()
		cursor.HeartTimeout = int64(ConsumerTimeout)
		err := r.Repo.StealCursor(cursor, r.cursor.Name)

		//Consumer is still alive, skip him.
		if err == redis.TxFailedErr {
			continue
		}

		if err != nil {
			return fmt.Errorf("identify: %s", err.Error())
		}

		r.cursor.LastID = cursor.LastID
		return nil
	}

	r.cursor.Name, r.cursor.LastID = getUniqueName(), "$"
	return nil
}

func (r RedisStreamer) isAckOverdue(ID string) bool {
	parts := strings.Split(ID, "-")

	millis, err := strconv.Atoi(parts[0])

	if err != nil {
		log.Printf("Error parsing ID to timestamp: %s\n", ID)
		return false
	}

	IDTime := time.Unix(
		0,
		int64(millis)*int64(time.Millisecond),
	)

	return r.Clock.Now().Sub(IDTime) > ConsumerTimeout
}

func getUniqueName() string {
	return uuid.NewV4().String()
}
