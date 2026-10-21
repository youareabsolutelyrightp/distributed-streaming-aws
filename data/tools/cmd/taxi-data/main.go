package main

import (
	"bufio"
	"container/heap"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

var tripColumns = []string{
	"medallion", "hack_license", "vendor_id", "rate_code",
	"store_and_fwd_flag", "pickup_datetime", "dropoff_datetime",
	"passenger_count", "trip_time_in_secs", "trip_distance",
	"pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
}

var tripOutputColumns = []string{
	"source_row_number", "medallion", "hack_license", "vendor_id", "rate_code",
	"store_and_fwd_flag", "pickup_datetime", "dropoff_datetime",
	"passenger_count", "trip_time_seconds", "trip_distance_miles",
	"pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
}

var fareColumns = []string{
	"medallion", "hack_license", "vendor_id", "pickup_datetime", "payment_type",
	"fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount",
}

var fareOutputColumns = []string{
	"source_row_number", "medallion", "hack_license", "vendor_id", "pickup_datetime",
	"payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount",
	"tolls_amount", "total_amount",
}

var monthlyColumns = []string{
	"month_id", "trip_source_checksum", "trip_original_row_number",
	"fare_source_checksum", "fare_original_row_number", "joined_record_id",
	"medallion", "hack_license", "vendor_id", "pickup_datetime", "dropoff_datetime",
	"rate_code", "store_and_fwd_flag", "passenger_count", "trip_time_seconds",
	"trip_distance_miles", "pickup_longitude", "pickup_latitude",
	"dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount",
	"surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount",
}

var preparedColumns = append([]string{
	"schema_version", "dataset_id", "event_id", "source_line_number",
}, monthlyColumns...)

type normalizeStats struct {
	Kind              string   `json:"kind"`
	SourceName        string   `json:"source_name"`
	SourceSHA256      string   `json:"source_sha256"`
	Header            []string `json:"header"`
	SourceRows        uint64   `json:"source_rows"`
	ValidRows         uint64   `json:"valid_rows"`
	MalformedRows     uint64   `json:"malformed_rows"`
	BlankPhysicalRows uint64   `json:"blank_physical_rows"`
}

type rejectRecord struct {
	SourceRowNumber uint64 `json:"source_row_number"`
	Reason          string `json:"reason"`
	RawSHA256       string `json:"raw_sha256"`
	Raw             string `json:"raw"`
}

func main() {
	if len(os.Args) < 2 {
		usageAndExit()
	}
	var err error
	switch os.Args[1] {
	case "normalize":
		err = normalizeCommand(os.Args[2:])
	case "merge":
		err = mergeCommand(os.Args[2:])
	case "verify":
		err = verifyCommand(os.Args[2:])
	case "verify-monthly":
		err = verifyMonthlyCommand(os.Args[2:])
	default:
		usageAndExit()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "taxi-data:", err)
		os.Exit(1)
	}
}

func usageAndExit() {
	fmt.Fprintln(os.Stderr, "usage: taxi-data <normalize|merge|verify|verify-monthly> [options]")
	os.Exit(2)
}

func normalizeCommand(args []string) error {
	fs := flag.NewFlagSet("normalize", flag.ContinueOnError)
	kind := fs.String("kind", "", "trip or fare")
	input := fs.String("input", "-", "input CSV or - for stdin")
	output := fs.String("output", "", "numbered valid CSV.zst")
	rejects := fs.String("rejects", "", "malformed JSONL.zst")
	statsPath := fs.String("stats", "", "normalization stats JSON")
	sourceName := fs.String("source-name", "", "logical source filename")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*kind != "trip" && *kind != "fare") || *output == "" || *rejects == "" || *statsPath == "" {
		return errors.New("normalize requires --kind trip|fare, --output, --rejects, and --stats")
	}

	var in io.ReadCloser
	if *input == "-" {
		in = os.Stdin
	} else {
		f, err := os.Open(*input)
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
		if *sourceName == "" {
			*sourceName = filepath.Base(*input)
		}
	}
	if *sourceName == "" {
		*sourceName = "stdin.csv"
	}

	for _, path := range []string{*output, *rejects, *statsPath} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
	}
	outTmp := *output + ".tmp"
	rejectTmp := *rejects + ".tmp"
	defer os.Remove(outTmp)
	defer os.Remove(rejectTmp)

	outFile, err := os.Create(outTmp)
	if err != nil {
		return err
	}
	outZstd, err := zstd.NewWriter(outFile, zstd.WithEncoderConcurrency(1))
	if err != nil {
		outFile.Close()
		return err
	}
	outCSV := csv.NewWriter(outZstd)

	rejectFile, err := os.Create(rejectTmp)
	if err != nil {
		outZstd.Close()
		outFile.Close()
		return err
	}
	rejectZstd, err := zstd.NewWriter(rejectFile, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	rejectJSON := json.NewEncoder(rejectZstd)
	rejectJSON.SetEscapeHTML(false)

	sourceHash := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(in, sourceHash), 4*1024*1024)
	headerLine, err := readPhysicalLine(reader)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	header, err := parseCSVLine(headerLine)
	if err != nil {
		return fmt.Errorf("parse header: %w", err)
	}
	for i := range header {
		header[i] = canonicalHeader(header[i])
	}
	required := fareColumns
	outputHeader := fareOutputColumns
	if *kind == "trip" {
		required = tripColumns
		outputHeader = tripOutputColumns
	}
	positions, err := headerPositions(header, required)
	if err != nil {
		return err
	}
	if err := outCSV.Write(outputHeader); err != nil {
		return err
	}

	stats := normalizeStats{Kind: *kind, SourceName: *sourceName, Header: header}
	for {
		line, readErr := readPhysicalLine(reader)
		if errors.Is(readErr, io.EOF) && len(line) == 0 {
			break
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		stats.SourceRows++
		if len(strings.TrimSpace(string(line))) == 0 {
			stats.BlankPhysicalRows++
			stats.MalformedRows++
			if err := writeReject(rejectJSON, stats.SourceRows, "blank_physical_line", line); err != nil {
				return err
			}
		} else {
			record, parseErr := parseCSVLine(line)
			if parseErr != nil || len(record) != len(header) {
				reason := "invalid_csv"
				if parseErr == nil {
					reason = fmt.Sprintf("field_count:%d_expected:%d", len(record), len(header))
				}
				stats.MalformedRows++
				if err := writeReject(rejectJSON, stats.SourceRows, reason, line); err != nil {
					return err
				}
			} else {
				canonical, validationErr := canonicalizeRecord(*kind, record, positions)
				if validationErr != nil {
					stats.MalformedRows++
					if err := writeReject(rejectJSON, stats.SourceRows, validationErr.Error(), line); err != nil {
						return err
					}
				} else {
					row := append([]string{strconv.FormatUint(stats.SourceRows, 10)}, canonical...)
					if err := outCSV.Write(row); err != nil {
						return err
					}
					stats.ValidRows++
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}

	outCSV.Flush()
	if err := outCSV.Error(); err != nil {
		return err
	}
	if err := outZstd.Close(); err != nil {
		return err
	}
	if err := outFile.Close(); err != nil {
		return err
	}
	if err := rejectZstd.Close(); err != nil {
		return err
	}
	if err := rejectFile.Close(); err != nil {
		return err
	}
	stats.SourceSHA256 = hex.EncodeToString(sourceHash.Sum(nil))
	if stats.ValidRows+stats.MalformedRows != stats.SourceRows {
		return errors.New("normalization accounting invariant failed")
	}
	if err := os.Rename(outTmp, *output); err != nil {
		return err
	}
	if err := os.Rename(rejectTmp, *rejects); err != nil {
		return err
	}
	if err := writeJSONAtomic(*statsPath, stats); err != nil {
		return err
	}
	fmt.Printf("normalized %s: %d valid, %d malformed\n", *sourceName, stats.ValidRows, stats.MalformedRows)
	return nil
}

func readPhysicalLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	return line, err
}

func parseCSVLine(line []byte) ([]string, error) {
	r := csv.NewReader(strings.NewReader(string(line)))
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	record, err := r.Read()
	if err != nil {
		return nil, err
	}
	if extra, extraErr := r.Read(); extraErr != io.EOF || extra != nil {
		return nil, errors.New("multiple CSV records in one physical line")
	}
	return record, nil
}

func canonicalHeader(value string) string {
	return strings.ToLower(strings.TrimSpace(strings.TrimPrefix(value, "\ufeff")))
}

func headerPositions(header, required []string) (map[string]int, error) {
	all := make(map[string]int, len(header))
	for i, name := range header {
		if _, duplicate := all[name]; duplicate {
			return nil, fmt.Errorf("duplicate header %q", name)
		}
		all[name] = i
	}
	result := make(map[string]int, len(required))
	for _, name := range required {
		position, ok := all[name]
		if !ok {
			return nil, fmt.Errorf("required header %q is missing", name)
		}
		result[name] = position
	}
	return result, nil
}

func canonicalizeRecord(kind string, record []string, pos map[string]int) ([]string, error) {
	value := func(name string) string { return strings.TrimSpace(record[pos[name]]) }
	requiredText := []string{"medallion", "hack_license", "vendor_id", "pickup_datetime"}
	for _, name := range requiredText {
		if value(name) == "" {
			return nil, fmt.Errorf("empty_required_field:%s", name)
		}
	}
	pickup, err := canonicalTime(value("pickup_datetime"))
	if err != nil {
		return nil, fmt.Errorf("invalid_pickup_datetime")
	}
	if kind == "fare" {
		moneyNames := []string{"fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"}
		money := make([]string, 0, len(moneyNames))
		for _, name := range moneyNames {
			v, moneyErr := canonicalFixedDecimal(value(name), 2)
			if moneyErr != nil {
				return nil, fmt.Errorf("invalid_%s", name)
			}
			money = append(money, v)
		}
		return append([]string{
			value("medallion"), value("hack_license"), value("vendor_id"), pickup,
			value("payment_type"),
		}, money...), nil
	}

	dropoff, err := canonicalTime(value("dropoff_datetime"))
	if err != nil {
		return nil, fmt.Errorf("invalid_dropoff_datetime")
	}
	integerNames := []string{"rate_code", "passenger_count", "trip_time_in_secs"}
	integers := make(map[string]string, len(integerNames))
	for _, name := range integerNames {
		v, intErr := canonicalInteger(value(name))
		if intErr != nil {
			return nil, fmt.Errorf("invalid_%s", name)
		}
		integers[name] = v
	}
	decimalNames := []string{"trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"}
	decimals := make(map[string]string, len(decimalNames))
	for _, name := range decimalNames {
		v, decimalErr := canonicalFloat(value(name))
		if decimalErr != nil {
			return nil, fmt.Errorf("invalid_%s", name)
		}
		decimals[name] = v
	}
	return []string{
		value("medallion"), value("hack_license"), value("vendor_id"), integers["rate_code"],
		value("store_and_fwd_flag"), pickup, dropoff, integers["passenger_count"],
		integers["trip_time_in_secs"], decimals["trip_distance"], decimals["pickup_longitude"],
		decimals["pickup_latitude"], decimals["dropoff_longitude"], decimals["dropoff_latitude"],
	}, nil
}

func canonicalTime(value string) (string, error) {
	formats := []string{"2006-01-02 15:04:05", time.RFC3339}
	for _, format := range formats {
		if parsed, err := time.Parse(format, value); err == nil {
			return parsed.UTC().Format("2006-01-02T15:04:05Z"), nil
		}
	}
	return "", errors.New("invalid timestamp")
}

func canonicalInteger(value string) (string, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(parsed, 10), nil
}

func canonicalFloat(value string) (string, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", err
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return "", errors.New("non-finite number")
	}
	return strconv.FormatFloat(parsed, 'f', -1, 64), nil
}

func canonicalFixedDecimal(value string, scale int) (string, error) {
	if value == "" {
		return "", errors.New("empty decimal")
	}
	sign := ""
	if value[0] == '-' || value[0] == '+' {
		if value[0] == '-' {
			sign = "-"
		}
		value = value[1:]
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" {
		return "", errors.New("invalid decimal")
	}
	for _, part := range parts {
		for _, char := range part {
			if char < '0' || char > '9' {
				return "", errors.New("invalid decimal")
			}
		}
	}
	integer := strings.TrimLeft(parts[0], "0")
	if integer == "" {
		integer = "0"
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	for len(fraction) > scale && strings.HasSuffix(fraction, "0") {
		fraction = strings.TrimSuffix(fraction, "0")
	}
	if len(fraction) > scale {
		return "", errors.New("too many decimal places")
	}
	fraction += strings.Repeat("0", scale-len(fraction))
	if integer == "0" && strings.Trim(fraction, "0") == "" {
		sign = ""
	}
	return sign + integer + "." + fraction, nil
}

func writeReject(encoder *json.Encoder, row uint64, reason string, raw []byte) error {
	digest := sha256.Sum256(raw)
	return encoder.Encode(rejectRecord{
		SourceRowNumber: row,
		Reason:          reason,
		RawSHA256:       hex.EncodeToString(digest[:]),
		Raw:             strings.TrimSuffix(strings.TrimSuffix(string(raw), "\n"), "\r"),
	})
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type monthlyReader struct {
	path        string
	file        *os.File
	decoder     *zstd.Decoder
	csv         *csv.Reader
	indices     map[string]int
	current     []string
	previous    sortKey
	hasPrevious bool
	rowNumber   uint64
}

type sortKey struct {
	dropoff string
	month   uint64
	tripRow uint64
	fareRow uint64
}

type mergeHeap []*monthlyReader

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return compareKeys(keyFor(h[i]), keyFor(h[j])) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(value any)    { *h = append(*h, value.(*monthlyReader)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	value := old[n-1]
	*h = old[:n-1]
	return value
}

func mergeCommand(args []string) error {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	var inputs stringList
	fs.Var(&inputs, "input", "sorted monthly CSV.zst (repeat 12 times)")
	output := fs.String("output", "", "prepared CSV.zst")
	report := fs.String("report", "", "merge report JSON")
	datasetID := fs.String("dataset-id", "", "dataset identifier")
	schemaVersion := fs.Uint("schema-version", 1, "event schema version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(inputs) != 12 || *output == "" || *report == "" || *datasetID == "" {
		return errors.New("merge requires exactly 12 --input values plus --output, --report, and --dataset-id")
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		return err
	}

	readers := make([]*monthlyReader, 0, 12)
	queue := mergeHeap{}
	for _, path := range inputs {
		reader, err := openMonthly(path)
		if err != nil {
			closeMonthlyReaders(readers)
			return err
		}
		readers = append(readers, reader)
		if err := reader.advance(); err != nil && !errors.Is(err, io.EOF) {
			closeMonthlyReaders(readers)
			return err
		} else if err == nil {
			heap.Push(&queue, reader)
		}
	}

	tmp := *output + ".tmp"
	defer os.Remove(tmp)
	outFile, err := os.Create(tmp)
	if err != nil {
		return err
	}
	compressedHash := sha256.New()
	compressedWriter := io.MultiWriter(outFile, compressedHash)
	encoder, err := zstd.NewWriter(compressedWriter, zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	uncompressed := &countingWriter{writer: encoder}
	outCSV := csv.NewWriter(uncompressed)
	if err := outCSV.Write(preparedColumns); err != nil {
		return err
	}

	var count uint64
	var minimum, maximum string
	var previous sortKey
	hasPrevious := false
	for queue.Len() > 0 {
		reader := heap.Pop(&queue).(*monthlyReader)
		key := keyFor(reader)
		if hasPrevious && compareKeys(previous, key) > 0 {
			return fmt.Errorf("global sort order violation at record %d", count+1)
		}
		count++
		joinedID := reader.value("joined_record_id")
		eventDigest := sha256.Sum256([]byte(*datasetID + joinedID))
		prefix := []string{
			strconv.FormatUint(uint64(*schemaVersion), 10), *datasetID,
			hex.EncodeToString(eventDigest[:]), strconv.FormatUint(count, 10),
		}
		row := append(prefix, reader.current...)
		if err := outCSV.Write(row); err != nil {
			return err
		}
		if count == 1 {
			minimum = key.dropoff
		}
		maximum = key.dropoff
		previous = key
		hasPrevious = true
		if err := reader.advance(); err != nil && !errors.Is(err, io.EOF) {
			return err
		} else if err == nil {
			heap.Push(&queue, reader)
		}
	}
	outCSV.Flush()
	if err := outCSV.Error(); err != nil {
		return err
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	if err := outFile.Close(); err != nil {
		return err
	}
	closeMonthlyReaders(readers)
	if err := os.Rename(tmp, *output); err != nil {
		return err
	}
	info, err := os.Stat(*output)
	if err != nil {
		return err
	}
	reportValue := map[string]any{
		"dataset_id":                  *datasetID,
		"schema_version":              *schemaVersion,
		"prepared_event_count":        count,
		"prepared_checksum":           hex.EncodeToString(compressedHash.Sum(nil)),
		"prepared_compressed_bytes":   info.Size(),
		"prepared_uncompressed_bytes": uncompressed.count,
		"minimum_event_time":          minimum,
		"maximum_event_time":          maximum,
		"sort_verified":               true,
		"compression":                 "zstd",
	}
	if err := writeJSONAtomic(*report, reportValue); err != nil {
		return err
	}
	fmt.Printf("merged %d records into %s\n", count, *output)
	return nil
}

type countingWriter struct {
	writer io.Writer
	count  uint64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.count += uint64(n)
	return n, err
}

func openMonthly(path string) (*monthlyReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1))
	if err != nil {
		file.Close()
		return nil, err
	}
	reader := csv.NewReader(bufio.NewReaderSize(decoder, 4*1024*1024))
	header, err := reader.Read()
	if err != nil {
		decoder.Close()
		file.Close()
		return nil, err
	}
	indices := make(map[string]int, len(header))
	for i, name := range header {
		indices[name] = i
	}
	if len(header) != len(monthlyColumns) {
		return nil, fmt.Errorf("%s has %d columns, expected %d", path, len(header), len(monthlyColumns))
	}
	for i, expected := range monthlyColumns {
		if header[i] != expected {
			return nil, fmt.Errorf("%s column %d is %q, expected %q", path, i+1, header[i], expected)
		}
	}
	return &monthlyReader{path: path, file: file, decoder: decoder, csv: reader, indices: indices}, nil
}

func (r *monthlyReader) value(name string) string { return r.current[r.indices[name]] }

func (r *monthlyReader) advance() error {
	row, err := r.csv.Read()
	if err != nil {
		return err
	}
	if len(row) != len(monthlyColumns) {
		return fmt.Errorf("%s contains a row with %d columns", r.path, len(row))
	}
	r.current = row
	key := keyFor(r)
	if r.hasPrevious && compareKeys(r.previous, key) > 0 {
		return fmt.Errorf(
			"monthly sort order violation in %s at data row %d: previous=%+v current=%+v",
			r.path, r.rowNumber+1, r.previous, key,
		)
	}
	r.rowNumber++
	r.previous = key
	r.hasPrevious = true
	return nil
}

func verifyMonthlyCommand(args []string) error {
	fs := flag.NewFlagSet("verify-monthly", flag.ContinueOnError)
	input := fs.String("input", "", "sorted monthly CSV.zst")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" {
		return errors.New("verify-monthly requires --input")
	}
	reader, err := openMonthly(*input)
	if err != nil {
		return err
	}
	defer closeMonthlyReaders([]*monthlyReader{reader})
	for {
		err := reader.advance()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	fmt.Printf("verified monthly sort: %s (%d rows)\n", *input, reader.rowNumber)
	return nil
}

func keyFor(reader *monthlyReader) sortKey {
	month, _ := strconv.ParseUint(reader.value("month_id"), 10, 64)
	tripRow, _ := strconv.ParseUint(reader.value("trip_original_row_number"), 10, 64)
	fareRow, _ := strconv.ParseUint(reader.value("fare_original_row_number"), 10, 64)
	return sortKey{dropoff: reader.value("dropoff_datetime"), month: month, tripRow: tripRow, fareRow: fareRow}
}

func compareKeys(left, right sortKey) int {
	if left.dropoff < right.dropoff {
		return -1
	}
	if left.dropoff > right.dropoff {
		return 1
	}
	if left.month < right.month {
		return -1
	}
	if left.month > right.month {
		return 1
	}
	if left.tripRow < right.tripRow {
		return -1
	}
	if left.tripRow > right.tripRow {
		return 1
	}
	if left.fareRow < right.fareRow {
		return -1
	}
	if left.fareRow > right.fareRow {
		return 1
	}
	return 0
}

func closeMonthlyReaders(readers []*monthlyReader) {
	for _, reader := range readers {
		reader.decoder.Close()
		reader.file.Close()
	}
}

func verifyCommand(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	input := fs.String("input", "", "prepared CSV.zst")
	manifestPath := fs.String("manifest", "", "data manifest JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" || *manifestPath == "" {
		return errors.New("verify requires --input and --manifest")
	}
	manifestBytes, err := os.ReadFile(*manifestPath)
	if err != nil {
		return err
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	datasetID, _ := manifest["dataset_id"].(string)
	expectedChecksum, _ := manifest["prepared_checksum"].(string)
	expectedCount, err := jsonUint64(manifest["prepared_event_count"])
	if err != nil {
		return err
	}
	checksum, err := fileSHA256(*input)
	if err != nil {
		return err
	}
	if checksum != expectedChecksum {
		return fmt.Errorf("prepared checksum mismatch: got %s expected %s", checksum, expectedChecksum)
	}

	file, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer decoder.Close()
	reader := csv.NewReader(bufio.NewReaderSize(decoder, 4*1024*1024))
	header, err := reader.Read()
	if err != nil {
		return err
	}
	if len(header) != len(preparedColumns) {
		return errors.New("prepared header length mismatch")
	}
	indices := map[string]int{}
	for i, name := range header {
		indices[name] = i
	}
	for i, expected := range preparedColumns {
		if header[i] != expected {
			return fmt.Errorf("prepared column %d mismatch", i+1)
		}
	}
	var count uint64
	var previous sortKey
	hasPrevious := false
	minimum, maximum := "", ""
	for {
		row, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
		count++
		if row[indices["dataset_id"]] != datasetID {
			return fmt.Errorf("dataset ID mismatch at row %d", count)
		}
		line, parseErr := strconv.ParseUint(row[indices["source_line_number"]], 10, 64)
		if parseErr != nil || line != count {
			return fmt.Errorf("source line mismatch at row %d", count)
		}
		joinedID := row[indices["joined_record_id"]]
		eventDigest := sha256.Sum256([]byte(datasetID + joinedID))
		if row[indices["event_id"]] != hex.EncodeToString(eventDigest[:]) {
			return fmt.Errorf("event ID mismatch at row %d", count)
		}
		key := sortKey{
			dropoff: row[indices["dropoff_datetime"]],
			month:   mustUint(row[indices["month_id"]]),
			tripRow: mustUint(row[indices["trip_original_row_number"]]),
			fareRow: mustUint(row[indices["fare_original_row_number"]]),
		}
		if hasPrevious && compareKeys(previous, key) > 0 {
			return fmt.Errorf("sort violation at row %d", count)
		}
		if count == 1 {
			minimum = key.dropoff
		}
		maximum = key.dropoff
		previous, hasPrevious = key, true
	}
	if count != expectedCount {
		return fmt.Errorf("count mismatch: got %d expected %d", count, expectedCount)
	}
	if value, _ := manifest["minimum_event_time"].(string); value != minimum {
		return errors.New("minimum event time mismatch")
	}
	if value, _ := manifest["maximum_event_time"].(string); value != maximum {
		return errors.New("maximum event time mismatch")
	}
	result := map[string]any{"verified": true, "dataset_id": datasetID, "prepared_event_count": count, "prepared_checksum": checksum}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
	return nil
}

func mustUint(value string) uint64 {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}

func jsonUint64(value any) (uint64, error) {
	switch typed := value.(type) {
	case float64:
		return uint64(typed), nil
	case json.Number:
		parsed, err := strconv.ParseUint(string(typed), 10, 64)
		return parsed, err
	default:
		return 0, errors.New("manifest prepared_event_count is missing")
	}
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func writeJSONAtomic(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
