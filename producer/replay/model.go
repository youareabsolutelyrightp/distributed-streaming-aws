package replay

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

var preparedColumns = []string{
	"schema_version", "dataset_id", "event_id", "source_line_number",
	"month_id", "trip_source_checksum", "trip_original_row_number",
	"fare_source_checksum", "fare_original_row_number", "joined_record_id",
	"medallion", "hack_license", "vendor_id", "pickup_datetime", "dropoff_datetime",
	"rate_code", "store_and_fwd_flag", "passenger_count", "trip_time_seconds",
	"trip_distance_miles", "pickup_longitude", "pickup_latitude",
	"dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount",
	"surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount",
}

var coordinateLexeme = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

type Manifest struct {
	DatasetID           string `json:"dataset_id"`
	EventSchemaVersion  int    `json:"event_schema_version"`
	PreparedFilename    string `json:"prepared_filename"`
	PreparedChecksum    string `json:"prepared_checksum"`
	PreparedEventCount  uint64 `json:"prepared_event_count"`
	MinimumEventTime    string `json:"minimum_event_time"`
	MaximumEventTime    string `json:"maximum_event_time"`
	GlobalSortVerified  bool   `json:"global_sort_verified"`
	PreparedCompression string `json:"prepared_compression"`
}

func LoadManifest(path string) (Manifest, error) {
	var manifest Manifest
	data, err := os.ReadFile(path)
	if err != nil {
		return manifest, fmt.Errorf("read manifest: %w", err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.DatasetID == "" || manifest.PreparedChecksum == "" || manifest.PreparedEventCount == 0 {
		return manifest, errors.New("manifest is missing dataset identity, checksum, or event count")
	}
	if manifest.EventSchemaVersion != 1 || !manifest.GlobalSortVerified || manifest.PreparedCompression != "zstd" {
		return manifest, errors.New("manifest is not a verified schema-version-1 zstd dataset")
	}
	return manifest, nil
}

type Event struct {
	RecordType                string          `json:"record_type"`
	SchemaVersion             int             `json:"schema_version"`
	DatasetID                 string          `json:"dataset_id"`
	EventID                   string          `json:"event_id"`
	SourceLineNumber          uint64          `json:"source_line_number"`
	MonthID                   uint64          `json:"month_id"`
	TripSourceChecksum        string          `json:"trip_source_checksum"`
	TripOriginalRowNumber     uint64          `json:"trip_original_row_number"`
	FareSourceChecksum        string          `json:"fare_source_checksum"`
	FareOriginalRowNumber     uint64          `json:"fare_original_row_number"`
	JoinedRecordID            string          `json:"joined_record_id"`
	Medallion                 string          `json:"medallion"`
	HackLicense               string          `json:"hack_license"`
	VendorID                  string          `json:"vendor_id"`
	PickupDatetime            string          `json:"pickup_datetime"`
	DropoffDatetime           string          `json:"dropoff_datetime"`
	RateCode                  string          `json:"rate_code"`
	StoreAndFwdFlag           string          `json:"store_and_fwd_flag"`
	PassengerCount            json.RawMessage `json:"passenger_count"`
	TripTimeSeconds           json.RawMessage `json:"trip_time_seconds"`
	TripDistanceMiles         json.RawMessage `json:"trip_distance_miles"`
	PickupLongitude           json.RawMessage `json:"pickup_longitude"`
	PickupLatitude            json.RawMessage `json:"pickup_latitude"`
	DropoffLongitude          json.RawMessage `json:"dropoff_longitude"`
	DropoffLatitude           json.RawMessage `json:"dropoff_latitude"`
	PaymentType               string          `json:"payment_type"`
	FareAmount                string          `json:"fare_amount"`
	Surcharge                 string          `json:"surcharge"`
	MTATax                    string          `json:"mta_tax"`
	TipAmount                 string          `json:"tip_amount"`
	TollsAmount               string          `json:"tolls_amount"`
	TotalAmount               string          `json:"total_amount"`
	ProducerIngestEpochMicros int64           `json:"producer_ingest_epoch_us"`
	ProducerSendEpochMicros   int64           `json:"producer_send_epoch_us"`
	RunID                     string          `json:"run_id"`
}

type PreparedSource struct {
	file      *os.File
	decoder   *zstd.Decoder
	reader    *csv.Reader
	index     map[string]int
	rowNumber uint64
}

func OpenPrepared(path string) (*PreparedSource, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open prepared stream: %w", err)
	}
	decoder, err := zstd.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open zstd stream: %w", err)
	}
	reader := csv.NewReader(bufio.NewReaderSize(decoder, 1<<20))
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		decoder.Close()
		_ = file.Close()
		return nil, fmt.Errorf("read prepared header: %w", err)
	}
	if len(header) != len(preparedColumns) {
		decoder.Close()
		_ = file.Close()
		return nil, fmt.Errorf("prepared header has %d unsupported fields", len(header))
	}
	index := make(map[string]int, len(header))
	for position, name := range header {
		if name != preparedColumns[position] {
			decoder.Close()
			_ = file.Close()
			return nil, fmt.Errorf("prepared column %d is %q, expected %q", position+1, name, preparedColumns[position])
		}
		index[name] = position
	}
	return &PreparedSource{file: file, decoder: decoder, reader: reader, index: index}, nil
}

func (source *PreparedSource) Close() error {
	source.decoder.Close()
	return source.file.Close()
}

func (source *PreparedSource) Read(runID string, ingestMicros int64) (Event, error) {
	row, err := source.reader.Read()
	if err != nil {
		return Event{}, err
	}
	source.rowNumber++
	if len(row) != len(source.index) {
		return Event{}, fmt.Errorf("prepared row has %d fields, expected %d", len(row), len(source.index))
	}
	value := func(name string) string { return row[source.index[name]] }
	parseUint := func(name string) (uint64, error) {
		parsed, parseErr := strconv.ParseUint(value(name), 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("parse %s: %w", name, parseErr)
		}
		return parsed, nil
	}
	schemaVersion, err := strconv.Atoi(value("schema_version"))
	if err != nil || schemaVersion != 1 {
		return Event{}, fmt.Errorf("unsupported schema_version %q", value("schema_version"))
	}
	sourceLine, err := parseUint("source_line_number")
	if err != nil {
		return Event{}, err
	}
	if sourceLine != source.rowNumber {
		return Event{}, fmt.Errorf(
			"source line %d found; expected contiguous line %d", sourceLine, source.rowNumber)
	}
	monthID, err := parseUint("month_id")
	if err != nil {
		return Event{}, err
	}
	tripRow, err := parseUint("trip_original_row_number")
	if err != nil {
		return Event{}, err
	}
	fareRow, err := parseUint("fare_original_row_number")
	if err != nil {
		return Event{}, err
	}
	number := func(name string) (json.RawMessage, error) {
		text := strings.TrimSpace(value(name))
		if text == "" {
			return json.RawMessage("null"), nil
		}
		if !json.Valid([]byte(text)) {
			return nil, fmt.Errorf("%s is not a JSON number: %q", name, text)
		}
		var decoded any
		if decodeErr := json.Unmarshal([]byte(text), &decoded); decodeErr != nil {
			return nil, fmt.Errorf("parse %s: %w", name, decodeErr)
		}
		if _, ok := decoded.(float64); !ok {
			return nil, fmt.Errorf("%s is not numeric: %q", name, text)
		}
		return json.RawMessage(text), nil
	}
	coordinate := func(name string) (json.RawMessage, error) {
		text := strings.TrimSpace(value(name))
		if text == "" {
			return json.RawMessage("null"), nil
		}
		if len(text) > 64 || !coordinateLexeme.MatchString(text) {
			return nil, fmt.Errorf("%s is not a bounded plain decimal lexeme: %q", name, text)
		}
		return json.RawMessage(text), nil
	}
	numericNames := []string{"passenger_count", "trip_time_seconds", "trip_distance_miles"}
	numbers := make(map[string]json.RawMessage, len(numericNames))
	for _, name := range numericNames {
		parsed, parseErr := number(name)
		if parseErr != nil {
			return Event{}, parseErr
		}
		numbers[name] = parsed
	}
	for _, name := range []string{
		"pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
	} {
		parsed, parseErr := coordinate(name)
		if parseErr != nil {
			return Event{}, parseErr
		}
		numbers[name] = parsed
	}
	event := Event{
		RecordType: "trip", SchemaVersion: schemaVersion, DatasetID: value("dataset_id"),
		EventID: value("event_id"), SourceLineNumber: sourceLine, MonthID: monthID,
		TripSourceChecksum: value("trip_source_checksum"), TripOriginalRowNumber: tripRow,
		FareSourceChecksum: value("fare_source_checksum"), FareOriginalRowNumber: fareRow,
		JoinedRecordID: value("joined_record_id"), Medallion: value("medallion"),
		HackLicense: value("hack_license"), VendorID: value("vendor_id"),
		PickupDatetime: value("pickup_datetime"), DropoffDatetime: value("dropoff_datetime"),
		RateCode: value("rate_code"), StoreAndFwdFlag: value("store_and_fwd_flag"),
		PassengerCount: numbers["passenger_count"], TripTimeSeconds: numbers["trip_time_seconds"],
		TripDistanceMiles: numbers["trip_distance_miles"], PickupLongitude: numbers["pickup_longitude"],
		PickupLatitude: numbers["pickup_latitude"], DropoffLongitude: numbers["dropoff_longitude"],
		DropoffLatitude: numbers["dropoff_latitude"], PaymentType: value("payment_type"),
		FareAmount: value("fare_amount"), Surcharge: value("surcharge"), MTATax: value("mta_tax"),
		TipAmount: value("tip_amount"), TollsAmount: value("tolls_amount"),
		TotalAmount: value("total_amount"), ProducerIngestEpochMicros: ingestMicros, RunID: runID,
	}
	if err := ValidateEventIdentity(event.DatasetID, event.JoinedRecordID, event.EventID); err != nil {
		return Event{}, fmt.Errorf("source line %d: %w", event.SourceLineNumber, err)
	}
	return event, nil
}

func IsEnd(err error) bool { return errors.Is(err, io.EOF) }
