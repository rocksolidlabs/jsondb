package main

import (
	"fmt"

	"github.com/rocksolidlabs/jsondb"
	"github.com/intwinelabs/logger"
)

var log = logger.New()

type Person struct {
	ID    string
	FName string
	LName string
	Age   int
}

func main() {

	db, err := jsondb.NewJSONDB(".", log, false)
	if err != nil {
		panic(err)
	}

	person := &Person{
		ID:    jsondb.GenIDFromSeeed("PhenixRizen"),
		FName: "Phenix",
		LName: "Rizen",
		Age:   96,
	}

	err = db.Put("people", person.ID, person)
	if err != nil {
		panic(err)
	}

	person = &Person{
		ID:    jsondb.GenIDFromSeeed("PhenixFallen"),
		FName: "Phenix",
		LName: "Fallen",
		Age:   69,
	}

	err = db.Put("people", person.ID, person)
	if err != nil {
		panic(err)
	}

	var people []*Person
	err = db.GetWhere("people", "FName", "Phenix", &people, -1)

	for _, p := range people {
		fmt.Printf("Name: %s %s, Age: %d\n", p.FName, p.LName, p.Age)
	}

}
