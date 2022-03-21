package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/minio/simdjson-go"
)

// ErrMissingRecordId - got a record without an identifier
var ErrMissingRecordId = fmt.Errorf("missing record identifier")

// ErrBlankRecordId - got a blank identifier
var ErrBlankRecordId = fmt.Errorf("blank/empty record identifier")

// ErrUnsupportedCpu - CPU not supported
var ErrUnsupportedCpu = fmt.Errorf("cpu not supported")

// ErrUnexpectedRecord - unexpected record
var ErrUnexpectedRecord = fmt.Errorf("unexpected record")

// RecordLoader - the interface
type RecordLoader interface {
	Validate() error
	First() (Record, error)
	Next() (Record, error)
	Done()
}

// Record - the record interface
type Record interface {
	Id() string
	Raw() []byte
}

// this is our loader implementation
type recordLoaderImpl struct {
	Buffer     []byte
	ParsedJson *simdjson.ParsedJson
	Current    simdjson.Iter
}

// this is our record implementation
type recordImpl struct {
	RecordId string
	RawBytes []byte
}

// NewRecordLoader - the factory
func NewRecordLoader(filename string) (RecordLoader, error) {

	// check to see if this CPU type is supported
	if simdjson.SupportedCPU() == false {
		return nil, ErrUnsupportedCpu
	}

	// read the file into memory
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return &recordLoaderImpl{Buffer: buf}, nil
}

// Validate - read all the records to ensure the file is valid
func (l *recordLoaderImpl) Validate() error {

	// get the first record and error out if bad. An EOF is OK, just means the file is empty
	_, err := l.First()
	if err != nil {
		// are we done
		if err == io.EOF {
			log.Printf("WARNING: EOF on first read, looks like an empty file")
			return nil
		} else {
			log.Printf("ERROR: validation failure on record index 0 (%s)", err.Error())
			return err
		}
	}

	// used for reporting
	recordIndex := 1

	// read all the records and bail on the first failure except EOF
	for {
		_, err = l.Next()

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				log.Printf("ERROR: validation failure on record index %d (%s)", recordIndex, err.Error())
				return err
			}
		}
		recordIndex++
	}

	// everything is OK
	return nil
}

func (l *recordLoaderImpl) First() (Record, error) {

	// parse the file contents
	pj, err := simdjson.Parse(l.Buffer, nil)
	if err != nil {
		return nil, err
	}

	// assign to implementation
	l.ParsedJson = pj

	// get an iterator
	l.Current = l.ParsedJson.Iter()

	// return the record
	return l.Next()
}

func (l *recordLoaderImpl) Next() (Record, error) {

	// we will assume only 1 root record for now
	t := l.Current.Advance()
	if t == simdjson.TypeRoot {

		element, err := l.Current.FindElement(nil, "id")
		if err != nil {
			return nil, err
		}

		value, err := element.Iter.StringCvt()
		if err != nil {
			return nil, err
		}

		return &recordImpl{RecordId: value, RawBytes: l.Buffer}, nil
	}

	// if it's not the root, return EOF
	return nil, io.EOF
}

func (l *recordLoaderImpl) Done() {
	// nothing to do
}

func (r *recordImpl) Id() string {
	return r.RecordId
}

func (r *recordImpl) Raw() []byte {
	return r.RawBytes
}

//
// end of file
//
