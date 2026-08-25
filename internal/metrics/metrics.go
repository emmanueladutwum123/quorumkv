// Package metrics renders measurements in the Prometheus text exposition
// format.
//
// It is deliberately a renderer rather than a metrics library. The values it
// reports already exist — atomic counters on the server, and a status snapshot
// from the consensus core — so a registry that owned a second copy of them would
// add a way for the two to disagree without adding any information. What was
// actually missing was a way to read them from outside the process.
//
// The format is a line-oriented text protocol, so implementing it costs a page
// and avoids a dependency tree an order of magnitude larger than this module.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Kind distinguishes a value that only ever increases from one that moves in
// both directions. Prometheus uses the distinction to decide whether a drop
// means "the process restarted" or "the number went down".
type Kind uint8

const (
	// Counter is monotonic within a process lifetime. By convention its name ends
	// in _total.
	Counter Kind = iota
	// Gauge is a current value that may rise or fall.
	Gauge
)

func (k Kind) String() string {
	if k == Counter {
		return "counter"
	}
	return "gauge"
}

// Label is one dimension of a sample.
type Label struct {
	Name  string
	Value string
}

// Sample is one measurement.
type Sample struct {
	Name   string
	Help   string
	Kind   Kind
	Value  float64
	Labels []Label
}

// Render writes samples in the Prometheus text exposition format.
//
// Samples sharing a name are emitted together under a single HELP and TYPE
// header, which the format requires — a scraper rejects a metric family whose
// declaration appears twice. Ordering is stable so that two scrapes of an
// unchanged process produce identical bytes, which makes the output diffable
// and the tests exact.
func Render(w io.Writer, samples []Sample) error {
	ordered := make([]Sample, len(samples))
	copy(ordered, samples)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Name != ordered[j].Name {
			return ordered[i].Name < ordered[j].Name
		}
		return labelKey(ordered[i].Labels) < labelKey(ordered[j].Labels)
	})

	var buf strings.Builder
	seen := ""
	for i, s := range ordered {
		if i == 0 || s.Name != seen {
			seen = s.Name
			if s.Help != "" {
				fmt.Fprintf(&buf, "# HELP %s %s\n", s.Name, escapeHelp(s.Help))
			}
			fmt.Fprintf(&buf, "# TYPE %s %s\n", s.Name, s.Kind)
		}
		buf.WriteString(s.Name)
		writeLabels(&buf, s.Labels)
		buf.WriteByte(' ')
		buf.WriteString(formatValue(s.Value))
		buf.WriteByte('\n')
	}
	_, err := io.WriteString(w, buf.String())
	return err
}

func labelKey(labels []Label) string {
	var b strings.Builder
	for _, l := range labels {
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
		b.WriteByte(',')
	}
	return b.String()
}

func writeLabels(b *strings.Builder, labels []Label) {
	if len(labels) == 0 {
		return
	}
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(l.Value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
}

// formatValue renders a float the way the exposition format expects.
//
// Whole numbers are printed without a decimal point because every value this
// package reports is a count or an index, and "17" reads better than "1.7e+01"
// when someone is curling the endpoint to see what a node is doing.
func formatValue(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func escapeLabelValue(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
