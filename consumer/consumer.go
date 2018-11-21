package consumer

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/antekresic/grs/domain"
)

//Printer consumes stream entries by printing them to stdout
type Printer struct {
	Streamer domain.EntryStreamer
}

//Consume prints the entry to stdout
func (p Printer) Consume(e domain.Entry) error {
	contents, err := json.MarshalIndent(e, "", "    ")

	if err != nil {
		return err
	}

	fmt.Println(string(contents))

	return err
}

//StartConsuming consumes all the entries it gets from entry repo
func (p Printer) StartConsuming() error {
	for {
		entries, err := p.Streamer.GetEntries()

		if err != nil {
			return err
		}

		for _, e := range entries {
			err = p.Consume(e)

			if err != nil {
				log.Println(err.Error())
				continue
			}

			err = p.Streamer.MarkEntryProcessed(e.ID)

			if err != nil {
				log.Println(err.Error())
				continue
			}
		}
	}
}
