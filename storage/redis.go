package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/antekresic/grs/domain"
	"github.com/go-redis/redis"
)

const (
	streamName       string        = "eventStream"
	consumerSet      string        = "consumers"
	lastPositionKey  string        = "lastPosition:"
	heartKey         string        = "heart:"
	entryField       string        = "entry"
	readCount        int64         = 10
	readBlock        time.Duration = 1 * time.Second
	faultyStreamName string        = "faultyStream"
)

//RedisClient is an interface to the 3rd party Redis client.
type RedisClient interface {
	XAdd(*redis.XAddArgs) *redis.StringCmd
	XRead(*redis.XReadArgs) *redis.XStreamSliceCmd
	TxPipeline() redis.Pipeliner
	Sort(set string, sort *redis.Sort) *redis.StringSliceCmd
	Watch(fn func(*redis.Tx) error, keys ...string) error
}

//RedisRepository is a Redis implementation of EntryRepository.
type RedisRepository struct {
	Client RedisClient

	name   string
	lastID string
}

//AddEntry stores entry into a Redis Stream.
func (r RedisRepository) AddEntry(e domain.Entry) error {

	content, err := json.Marshal(e)

	if err != nil {
		return fmt.Errorf("AddEntry: %s", err)
	}

	m := map[string]interface{}{entryField: content}

	err = r.Client.XAdd(&redis.XAddArgs{
		Stream: streamName,
		Values: m,
	}).Err()

	if err != nil {
		return fmt.Errorf("AddEntry: %s", err)
	}

	return nil
}

//StoreCursor saves the data necessary to keep track of the streamers last position
func (r RedisRepository) StoreCursor(cursor domain.StreamCursor) error {
	pipe := r.Client.TxPipeline()

	pipe.SAdd(consumerSet, cursor.Name)
	pipe.Set(lastPosition(cursor.Name), cursor.LastID, time.Duration(0))
	pipe.Set(heart(cursor.Name), 1, time.Duration(cursor.HeartTimeout))

	_, err := pipe.Exec()

	if err != nil {
		return fmt.Errorf("StoreCursor: %s", err)
	}

	return nil
}

//GetEntries fetches events from Redis Stream.
func (r *RedisRepository) GetEntries(lastID string) (entries []domain.Entry, newLastID string, err error) {
	streams, err := r.Client.XRead(&redis.XReadArgs{
		Streams: []string{streamName, lastID},
		Count:   readCount,
		Block:   readBlock,
	}).Result()

	if err == redis.Nil {
		return nil, "", nil
	}

	if err != nil {
		return nil, "", fmt.Errorf("GetEntries: %s", err)
	}

	stream := getStreamByName(streamName, streams)

	if stream == nil {
		return nil, "", errors.New("GetEntries: Stream not found")
	}

	entries, newLastID = r.parseEntries(stream.Messages)

	return entries, newLastID, nil
}

func (r RedisRepository) parseEntries(mm []redis.XMessage) ([]domain.Entry, string) {
	results := make([]domain.Entry, 0, len(mm))
	tmpEntry := domain.Entry{}
	var lastID string

	for _, m := range mm {
		lastID = m.ID
		entry, ok := m.Values[entryField]

		if !ok {
			log.Printf("Failed getting entry from XMessage for ID: %s", m.ID)
			r.handleFaultyEntry(m.ID, m.Values)
			continue
		}

		entryString, ok := entry.(string)

		if !ok {
			log.Printf("Failed converting entry to string from XMessage for ID: %s", m.ID)
			r.handleFaultyEntry(m.ID, m.Values)
			continue
		}

		err := json.Unmarshal([]byte(entryString), &tmpEntry)

		if err != nil {
			log.Printf("Failed unmarshaling entry from XMessage for ID: %s", m.ID)
			r.handleFaultyEntry(m.ID, m.Values)
			continue
		}

		tmpEntry.ID = m.ID

		results = append(results, tmpEntry)
	}

	return results, lastID
}

func (r RedisRepository) handleFaultyEntry(ID string, values map[string]interface{}) {
	pipe := r.Client.TxPipeline()

	//XDel is not in an official version of the library.
	//There is a PR merged in master that adds the support for this command.
	//pipe.XDel(streamName, ID)
	pipe.XAdd(&redis.XAddArgs{
		Stream: faultyStreamName,
		Values: values,
	})

	_, err := pipe.Exec()

	if err != nil {
		log.Printf("Error handling faulty entry: %s\n", err)
	}
}

func getStreamByName(name string, ss []redis.XStream) *redis.XStream {
	for i, s := range ss {
		if s.Stream == name {
			return &ss[i]
		}
	}

	return nil
}

//GetCursors fetches all the information about cursors from Redis.
func (r RedisRepository) GetCursors() (cursors []domain.StreamCursor, err error) {
	results, err := r.Client.Sort(consumerSet, &redis.Sort{
		By: lastPosition("*"),
		Get: []string{
			heart("*"),
			"#",
			lastPosition("*"),
		},
		Alpha: true,
	}).Result()

	if err == redis.Nil {
		return []domain.StreamCursor{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("GetCursors: %s", err)
	}

	if len(results) < 3 {
		return []domain.StreamCursor{}, nil
	}

	cursors = make([]domain.StreamCursor, len(results)/3)

	i := 0

	for len(results) >= 3 {
		cursors[i].HasHeart = results[0] != ""
		cursors[i].Name = results[1]
		cursors[i].LastID = results[2]
		results = results[3:]
		i++
	}

	return cursors, nil
}

//StealCursor trys to get the identity and last position from an existing consumer.
//Returns redis.TxFailedErr if transaction fails which means that the consumer is alive.
func (r RedisRepository) StealCursor(oldCursor domain.StreamCursor, newConsumerName string) error {
	return r.Client.Watch(func(tx *redis.Tx) error {
		lastPositionID, err := tx.Get(lastPosition(oldCursor.Name)).Result()
		if err != nil {
			return fmt.Errorf("StealCursor: %s", err)
		}

		//If last position changed, fail the transaction.
		if lastPositionID != oldCursor.LastID {
			return redis.TxFailedErr
		}

		_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
			pipe.SRem(consumerSet, oldCursor.Name)
			pipe.Del(lastPosition(oldCursor.Name))
			pipe.SAdd(consumerSet, newConsumerName)
			pipe.Set(lastPosition(newConsumerName), lastPositionID, time.Duration(0))
			pipe.Set(heart(newConsumerName), 1, time.Duration(oldCursor.HeartTimeout))
			return nil

		})

		return err

	}, lastPosition(oldCursor.Name))
}

func lastPosition(ID string) string {
	return lastPositionKey + ID
}

func heart(ID string) string {
	return heartKey + ID
}
