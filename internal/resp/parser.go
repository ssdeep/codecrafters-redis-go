package resp

import (
	"bytes"
	"container/list"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

const CRLF = "\r\n"
const CR = '\r'
const LF = '\n'
const SP = ' '

type Value struct {
	Name   string
	Expiry int64
}

type Entry struct {
	ID     string
	Values *sync.Map
}

type ID struct {
	Millis int
	Seq    int
}

func (e Entry) IdSplits() (ID, error) {
	id_splits := strings.Split(e.ID, "-")
	millis, err := strconv.Atoi(id_splits[0])
	if err != nil {
		fmt.Println("Error parsing id millisecond part: ", id_splits[0], err.Error())
		return ID{0, 0}, err
	}
	seq, err := strconv.Atoi(id_splits[1])
	if err != nil {
		fmt.Println("Error parsing id sequence part: ", id_splits[1], err.Error())
		return ID{0, 0}, err
	}
	return ID{millis, seq}, nil
}

type RespDataType int

type RespDataTypeParser interface {
	Parse(line []byte) (string, error)
}

type SimpleStringParser struct{}
type SimpleErrorsParser struct{}
type IntegersParser struct{}
type BulkStringsParser struct{}
type NullBulkStringsParser struct{}
type ArraysParser struct{}
type NullsParser struct{}
type BooleansParser struct{}
type DoublesParser struct{}
type BigNumbersParser struct{}
type BulkErrorsParser struct{}
type VerbatimStringsParser struct{}
type MapsParser struct{}
type AtrributesParser struct{}
type SetsParser struct{}
type PushesParser struct{}

type RawStringParser struct{}

const (
	SimpleString RespDataType = iota
	SimpleErrors
	Integers
	BulkStrings
	NullBulkStrings
	Arrays
	Nulls
	Booleans
	Doubles
	BigNumbers
	BulkErrors
	VerbatimStrings
	Maps
	Attributes
	Sets
	Pushes
	RawString
)

type RespParser interface {
	Parse(line []byte) (string, error)
}

// stripFirstAndCRLF strips the type byte prefix and trailing \r\n.
func stripFirstAndCRLF(line []byte) (string, error) {
	if bytes.Compare([]byte(CRLF), line[len(line)-len(CRLF):]) != 1 {
		return "", fmt.Errorf("invalid line: %s", line)
	}
	return string(line[1 : len(line)-len(CRLF)]), nil
}

func (r SimpleStringParser) Parse(line []byte) (string, error) {
	return stripFirstAndCRLF(line)
}

func (r RawStringParser) Parse(line []byte) (string, error) {
	return string(line), nil
}

func (r SimpleErrorsParser) Parse(line []byte) (string, error) {
	return stripFirstAndCRLF(line)
}

func (r IntegersParser) Parse(line []byte) (string, error) {
	return stripFirstAndCRLF(line)
}

// BulkStringsParser handles $<len>\r\n<data>\r\n and null $-1\r\n.
func (r BulkStringsParser) Parse(line []byte) (string, error) {
	crlfIdx := bytes.Index(line, []byte(CRLF))
	if crlfIdx == -1 {
		return "", fmt.Errorf("invalid bulk string: no CRLF")
	}
	n, err := strconv.Atoi(string(line[1:crlfIdx]))
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %w", err)
	}
	if n == -1 {
		return "", nil
	}
	data := line[crlfIdx+2 : crlfIdx+2+n]
	return string(data), nil
}

func EncodeBulkString(s string) []byte {
	data := []byte(s)
	return fmt.Appendf(nil, "$%d%s%s%s", len(data), CRLF, data, CRLF)
}

func EncodeSimpleString(s string) []byte {
	return []byte("+" + s + CRLF)
}

func (r IntegersParser) Encode(s int) []byte {
	return fmt.Appendf(nil, ":%d%s", s, CRLF)
}

func EncodeError(s string) []byte {
	return []byte("-" + s + CRLF)
}

// NullBulkStringsParser handles $-1\r\n.
func (r NullBulkStringsParser) Parse(_ []byte) (string, error) {
	return "", nil
}

// NullsParser handles _\r\n.
func (r NullsParser) Parse(_ []byte) (string, error) {
	return "", nil
}

// BooleansParser handles #t\r\n and #f\r\n.
func (r BooleansParser) Parse(line []byte) (string, error) {
	if string(line[len(line)-2:]) != CRLF {
		return "", fmt.Errorf("invalid boolean: %s", line)
	}
	switch line[1] {
	case 't':
		return "true", nil
	case 'f':
		return "false", nil
	default:
		return "", fmt.Errorf("invalid boolean value: %q", line[1])
	}
}

// DoublesParser handles ,<value>\r\n.
func (r DoublesParser) Parse(line []byte) (string, error) {
	return stripFirstAndCRLF(line)
}

// BigNumbersParser handles (<value>\r\n.
func (r BigNumbersParser) Parse(line []byte) (string, error) {
	return stripFirstAndCRLF(line)
}

// BulkErrorsParser handles !<len>\r\n<data>\r\n — same layout as bulk strings.
func (r BulkErrorsParser) Parse(line []byte) (string, error) {
	return BulkStringsParser{}.Parse(line)
}

// VerbatimStringsParser handles =<len>\r\n<enc>:<data>\r\n.
// The first 4 bytes of the payload are the encoding prefix (e.g. "txt:").
func (r VerbatimStringsParser) Parse(line []byte) (string, error) {
	crlfIdx := bytes.Index(line, []byte(CRLF))
	if crlfIdx == -1 {
		return "", fmt.Errorf("invalid verbatim string: no CRLF")
	}
	n, err := strconv.Atoi(string(line[1:crlfIdx]))
	if err != nil {
		return "", fmt.Errorf("invalid verbatim string length: %w", err)
	}
	payload := line[crlfIdx+2 : crlfIdx+2+n] // e.g. "txt:hello"
	if len(payload) < 4 {
		return "", fmt.Errorf("verbatim string payload too short")
	}
	return string(payload[4:]), nil // skip "txt:" prefix
}

// ArraysParser handles *<count>\r\n followed by count RESP values.
func (r ArraysParser) Parse(line []byte) (string, error) {
	return parseAggregate(line, 1)
}

func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

func (r ArraysParser) Encode(l list.List) []byte {
	var buf bytes.Buffer
	leng := l.Len()
	fmt.Fprintf(&buf, "*%d%s", leng, CRLF)
	if l.Len() > 0 {
		items := l.Front()
		for range leng {
			if val, ok := items.Value.(Value); ok {
				buf.Write(EncodeBulkString(val.Name))
			}
			//ok {
			//	buf.Write(EncodeBulkString(val))
			//}
			items = items.Next()
		}
	}
	return buf.Bytes()
}

// MapsParser handles %<count>\r\n followed by count*2 RESP values (key/value pairs).
func (r MapsParser) Parse(line []byte) (string, error) {
	return parseAggregate(line, 2)
}

// AtrributesParser handles |<count>\r\n followed by count*2 RESP values.
func (r AtrributesParser) Parse(line []byte) (string, error) {
	return parseAggregate(line, 2)
}

// SetsParser handles ~<count>\r\n followed by count RESP values.
func (r SetsParser) Parse(line []byte) (string, error) {
	return parseAggregate(line, 1)
}

// PushesParser handles ><count>\r\n followed by count RESP values.
func (r PushesParser) Parse(line []byte) (string, error) {
	return parseAggregate(line, 1)
}

// parseAggregate parses an aggregate type. multiplier is 1 for arrays/sets/pushes,
// 2 for maps/attributes (each "count" entry is a key+value pair).
func parseAggregate(line []byte, multiplier int) (string, error) {
	crlfIdx := bytes.Index(line, []byte(CRLF))
	if crlfIdx == -1 {
		return "", fmt.Errorf("invalid aggregate: no CRLF")
	}
	count, err := strconv.Atoi(string(line[1:crlfIdx]))
	if err != nil {
		return "", fmt.Errorf("invalid aggregate count: %w", err)
	}
	total := count * multiplier
	parts, err := parseSubValues(line[crlfIdx+2:], total)
	if err != nil {
		return "", err
	}
	return strings.Join(parts, " "), nil
}

// valueEnd returns how many bytes the first complete RESP value in data occupies.
func valueEnd(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("empty data")
	}
	crlfIdx := bytes.Index(data, []byte(CRLF))
	if crlfIdx == -1 {
		return 0, fmt.Errorf("no CRLF found")
	}
	switch data[0] {
	case '+', '-', ':', '_', '#', ',', '(':
		return crlfIdx + 2, nil
	case '$', '!':
		n, err := strconv.Atoi(string(data[1:crlfIdx]))
		if err != nil || n == -1 {
			return crlfIdx + 2, nil
		}
		return crlfIdx + 2 + n + 2, nil
	case '=':
		n, err := strconv.Atoi(string(data[1:crlfIdx]))
		if err != nil {
			return 0, err
		}
		return crlfIdx + 2 + n + 2, nil
	case '*', '~', '>':
		count, err := strconv.Atoi(string(data[1:crlfIdx]))
		if err != nil {
			return 0, err
		}
		offset := crlfIdx + 2
		for i := 0; i < count; i++ {
			end, err := valueEnd(data[offset:])
			if err != nil {
				return 0, err
			}
			offset += end
		}
		return offset, nil
	case '%', '|':
		count, err := strconv.Atoi(string(data[1:crlfIdx]))
		if err != nil {
			return 0, err
		}
		offset := crlfIdx + 2
		for i := 0; i < count*2; i++ {
			end, err := valueEnd(data[offset:])
			if err != nil {
				return 0, err
			}
			offset += end
		}
		return offset, nil
	}
	return 0, fmt.Errorf("unknown type byte: %q", data[0])
}

// parseSubValues parses n consecutive RESP values from data.
func parseSubValues(data []byte, n int) ([]string, error) {
	results := make([]string, 0, n)
	offset := 0
	for i := 0; i < n; i++ {
		end, err := valueEnd(data[offset:])
		if err != nil {
			return nil, err
		}
		val, err := ParseLine(data[offset : offset+end])
		if err != nil {
			return nil, err
		}
		results = append(results, val)
		offset += end
	}
	return results, nil
}

var ByFirstChar = map[byte]RespDataTypeParser{
	'+': SimpleStringParser{},
	'-': SimpleErrorsParser{},
	':': IntegersParser{},
	'$': BulkStringsParser{},
	'*': ArraysParser{},
	'_': NullsParser{},
	'#': BooleansParser{},
	',': DoublesParser{},
	'(': BigNumbersParser{},
	'!': BulkErrorsParser{},
	'=': VerbatimStringsParser{},
	'%': MapsParser{},
	'|': AtrributesParser{},
	'~': SetsParser{},
	'>': PushesParser{},
}

func Type(b byte) RespDataTypeParser {
	str, err := ByFirstChar[b]
	if !err || (str == nil) {
		fmt.Printf("unknown type byte: %q\n", b)
		return RawStringParser{}
	} else {
		fmt.Printf("type byte: %T\n", str)
		return str
	}
}

func ParseLine(line []byte) (string, error) {
	strType := Type(line[0])
	word, err := strType.Parse(line)
	return word, err
}

func Parse(con net.Conn) (string, error) {
	content := make([]byte, 1024)
	l, err := con.Read(content)
	if err != nil {
		return "", err
	}
	return ParseLine(content[:l])
}
