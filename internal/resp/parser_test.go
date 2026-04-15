package resp

import (
	"net"
	"testing"
)

func TestParse(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		client.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	}()

	result, err := Parse(server)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != "PING" {
		t.Errorf("expected %q, got %q", "PING", result)
	}
}

func TestBulkStringsParser(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"simple word", []byte("$4\r\nPING\r\n"), "PING"},
		{"empty string", []byte("$0\r\n\r\n"), ""},
		{"null bulk string", []byte("$-1\r\n"), ""},
	}
	p := BulkStringsParser{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestNullBulkStringsParser(t *testing.T) {
	got, err := NullBulkStringsParser{}.Parse([]byte("$-1\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestNullsParser(t *testing.T) {
	got, err := NullsParser{}.Parse([]byte("_\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestBooleansParser(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte("#t\r\n"), "true"},
		{[]byte("#f\r\n"), "false"},
	}
	p := BooleansParser{}
	for _, tt := range tests {
		got, err := p.Parse(tt.input)
		if err != nil {
			t.Fatalf("input %q: unexpected error: %v", tt.input, err)
		}
		if got != tt.expected {
			t.Errorf("input %q: expected %q, got %q", tt.input, tt.expected, got)
		}
	}
}

func TestDoublesParser(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte(",1.5\r\n"), "1.5"},
		{[]byte(",inf\r\n"), "inf"},
		{[]byte(",-inf\r\n"), "-inf"},
	}
	p := DoublesParser{}
	for _, tt := range tests {
		got, err := p.Parse(tt.input)
		if err != nil {
			t.Fatalf("input %q: unexpected error: %v", tt.input, err)
		}
		if got != tt.expected {
			t.Errorf("input %q: expected %q, got %q", tt.input, tt.expected, got)
		}
	}
}

func TestBigNumbersParser(t *testing.T) {
	got, err := BigNumbersParser{}.Parse([]byte("(3492890328409238509324850943850943825024385\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "3492890328409238509324850943850943825024385"
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestBulkErrorsParser(t *testing.T) {
	got, err := BulkErrorsParser{}.Parse([]byte("!21\r\nSYNTAX invalid syntax\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "SYNTAX invalid syntax" {
		t.Errorf("expected %q, got %q", "SYNTAX invalid syntax", got)
	}
}

func TestVerbatimStringsParser(t *testing.T) {
	got, err := VerbatimStringsParser{}.Parse([]byte("=9\r\ntxt:hello\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hello" {
		t.Errorf("expected %q, got %q", "hello", got)
	}
}

func TestArraysParser(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			"two bulk strings",
			[]byte("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"),
			"foo bar",
		},
		{
			"set command",
			[]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
			"SET key value",
		},
		{
			"empty array",
			[]byte("*0\r\n"),
			"",
		},
	}
	p := ArraysParser{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestMapsParser(t *testing.T) {
	// %1\r\n$3\r\nfoo\r\n:42\r\n  →  "foo 42"
	got, err := MapsParser{}.Parse([]byte("%1\r\n$3\r\nfoo\r\n:42\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "foo 42" {
		t.Errorf("expected %q, got %q", "foo 42", got)
	}
}

func TestSetsParser(t *testing.T) {
	got, err := SetsParser{}.Parse([]byte("~3\r\n+a\r\n+b\r\n+c\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "a b c" {
		t.Errorf("expected %q, got %q", "a b c", got)
	}
}

func TestPushesParser(t *testing.T) {
	got, err := PushesParser{}.Parse([]byte(">2\r\n+message\r\n$5\r\nhello\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "message hello" {
		t.Errorf("expected %q, got %q", "message hello", got)
	}
}

func TestAtrributesParser(t *testing.T) {
	// |1\r\n$4\r\nname\r\n$5\r\nRedis\r\n  →  "name Redis"
	got, err := AtrributesParser{}.Parse([]byte("|1\r\n$4\r\nname\r\n$5\r\nRedis\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "name Redis" {
		t.Errorf("expected %q, got %q", "name Redis", got)
	}
}