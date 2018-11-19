package storage

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/antekresic/grs/domain"
	"github.com/go-redis/redis"
)

const (
	streamName    string = "eventStream"
	groupName     string = "consumerGroup"
	startPosition string = "0"
	entryField    string = "entry"

	//prefix for "group already exists" error which
	//can occur when creating a stream group
	groupExistsAlreadyPrefix string = "BUSYGROUP "
)

var once sync.Once

//RedisRepository is a Redis implementation of EntryRepository
type RedisRepository struct {
	Client       *redis.Client
	ConsumerName string

	groupCreated bool
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

//CreateStreamGroup creates stream group for consumers
func (r RedisRepository) CreateStreamGroup() error {
	//result := r.Client.XGroupCreate(streamName, groupName, startPosition)
	result := redis.NewStatusCmd("xgroup", "create", streamName, groupName, startPosition, "mkstream")
	r.Client.Process(result)

	err := result.Err()

	//return error unless its the "group already exists" error
	if err != nil && !strings.HasPrefix(err.Error(), groupExistsAlreadyPrefix) {
		return err
	}

	return nil
}

//Ack acknowledges that the entry was processed
func (r RedisRepository) Ack(e domain.Entry) error {
	return r.Client.XAck(streamName, groupName, e.ID).Err()
}

//GetEntries fetches events from Redis Stream
func (r RedisRepository) GetEntries() ([]domain.Entry, error) {
	var err error
	once.Do(func() {
		err = r.CreateStreamGroup()
	})

	if err != nil {
		return nil, err
	}

	return r.getEntries(r.ConsumerName)
}

func (r RedisRepository) getEntries(consumer string) ([]domain.Entry, error) {
	streams, err := r.Client.XReadGroup(&redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumer,
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    1 * time.Second,
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

	return getEntries(stream.Messages), nil
}

func getEntries(mm []redis.XMessage) []domain.Entry {
	results := make([]domain.Entry, 0, len(mm))
	tmpEntry := domain.Entry{}

	for _, m := range mm {
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

	return results
}

func getStreamByName(name string, ss []redis.XStream) *redis.XStream {
	for i, s := range ss {
		if s.Stream == name {
			return &ss[i]
		}
	}

	return nil
}
