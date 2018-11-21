package storage

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/antekresic/grs/domain"
	"github.com/go-redis/redis"
	uuid "github.com/satori/go.uuid"
)

const (
	streamName      string        = "eventStream"
	cursorHash      string        = "lastPositionCursors"
	entryField      string        = "entry"
	readCount       int64         = 10
	readBlock       time.Duration = 1 * time.Second
	consumerTimeout time.Duration = 5 * time.Second

	//prefix for "group already exists" error which
	//can occur when creating a stream group
	groupExistsAlreadyPrefix string = "BUSYGROUP "
)

//RedisRepository is a Redis implementation of EntryRepository
type RedisRepository struct {
	Client *redis.Client

	name   string
	lastID string
}

//AddEntry stores entry into a Redis Stream
func (r RedisRepository) AddEntry(e domain.Entry) error {

	content, err := json.Marshal(e)

	if err != nil {
		return err
	}

	m := map[string]interface{}{entryField: content}

	res := r.Client.XAdd(&redis.XAddArgs{
		Stream: streamName,
		Values: m,
	})

	return res.Err()
}

//Ack acknowledges that the entry was processed by the consumer
func (r RedisRepository) Ack(ID string) error {
	pipe := r.Client.TxPipeline()

	pipe.SAdd("consumers", r.name)
	pipe.Set("lastPosition:"+r.name, ID, time.Duration(0))
	pipe.Set("heart:"+r.name, 1, consumerTimeout)

	_, err := pipe.Exec()

	return err
}

//GetEntries fetches events from Redis Stream
func (r *RedisRepository) GetEntries() ([]domain.Entry, error) {
	if r.name == "" {
		err := r.identify()

		if err != nil {
			return nil, err
		}
	}

	return r.getEntries(r.lastID)
}

func (r *RedisRepository) getEntries(lastID string) ([]domain.Entry, error) {
	streams, err := r.Client.XRead(&redis.XReadArgs{
		Streams: []string{streamName, lastID},
		Count:   readCount,
		Block:   readBlock,
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	stream := getStreamByName(streamName, streams)

	if stream == nil {
		return nil, errors.New("Stream not found")
	}

	entries, lastID := getEntries(stream.Messages)

	if lastID != "" {
		r.lastID = lastID
	}

	return entries, nil
}

func getEntries(mm []redis.XMessage) ([]domain.Entry, string) {
	results := make([]domain.Entry, 0, len(mm))
	tmpEntry := domain.Entry{}
	var lastID string

	for _, m := range mm {
		lastID = m.ID
		entry, ok := m.Values[entryField]

		if !ok {
			log.Printf("Failed getting entry from XMessage for ID: %s", m.ID)
			continue
		}

		entryString, ok := entry.(string)

		if !ok {
			log.Printf("Failed converting entry to string from XMessage for ID: %s", m.ID)
			continue
		}

		err := json.Unmarshal([]byte(entryString), &tmpEntry)

		if err != nil {
			log.Printf("Failed unmarshaling entry from XMessage for ID: %s", m.ID)
			continue
		}

		tmpEntry.ID = m.ID

		results = append(results, tmpEntry)
	}

	return results, lastID
}

func getStreamByName(name string, ss []redis.XStream) *redis.XStream {
	for i, s := range ss {
		if s.Stream == name {
			return &ss[i]
		}
	}

	return nil
}

//identify trys to assume the name and position of the consumer which has stopped
func (r *RedisRepository) identify() error {
	results, err := r.Client.Sort("consumers", &redis.Sort{
		By: "lastPosition:*",
		Get: []string{
			"heart:*",
			"#",
			"lastPosition:*",
		},
		Alpha: true,
	}).Result()

	if err != nil {
		return err
	}

	for {
		//no (more) consumer details
		if len(results) < 3 {
			break
		}

		//skip consumer which is still alive
		if results[0] != "" {
			results = results[3:]
			continue
		}

		r.name = getUniqueName()
		err := r.stealIdentity(results[1], results[2], r.name)

		//consumer is still alive, skip him
		if err == redis.TxFailedErr {
			results = results[3:]
			continue
		}

		if err != nil {
			return err
		}

		r.lastID = results[2]
		return nil
	}

	r.name, r.lastID = getUniqueName(), "$"
	return nil
}

//stealIdentity trys to get the identity and last position from an existing consumer
//returns redis.TxFailedErr if transaction fails which means that the consumer is alive
func (r RedisRepository) stealIdentity(oldConsumerName, ID, newConsumerName string) error {
	return r.Client.Watch(func(tx *redis.Tx) error {
		lastPosition, err := tx.Get("lastPosition:" + oldConsumerName).Result()
		if err != nil {
			return err
		}

		//if last position changed, fail the transaction
		if lastPosition != ID {
			return redis.TxFailedErr
		}

		_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
			pipe.SRem("consumers", oldConsumerName)
			pipe.Del("lastPosition:" + oldConsumerName)
			pipe.SAdd("consumers", newConsumerName)
			pipe.Set("lastPosition:"+newConsumerName, lastPosition, time.Duration(0))
			pipe.Set("heart:"+newConsumerName, 1, consumerTimeout)
			return nil

		})

		return err

	}, "lastPosition:"+oldConsumerName)
}

func getUniqueName() string {
	return uuid.NewV4().String()
}
