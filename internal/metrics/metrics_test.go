package metrics

import (
	"math"
	"strings"
	"testing"
)

func render(t testing.TB, samples ...Sample) string {
	t.Helper()
	var b strings.Builder
	if err := Render(&b, samples); err != nil {
		t.Fatalf("render: %v", err)
	}
	return b.String()
}

func TestEmptyInputRendersNothing(t *testing.T) {
	if got := render(t); got != "" {
		t.Errorf("rendered %q for no samples, want empty", got)
	}
}

func TestSingleSampleCarriesHelpAndType(t *testing.T) {
	got := render(t, Sample{Name: "quorumkv_elections_total", Help: "Elections started.", Kind: Counter, Value: 3})
	want := "# HELP quorumkv_elections_total Elections started.\n" +
		"# TYPE quorumkv_elections_total counter\n" +
		"quorumkv_elections_total 3\n"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestHeadersAppearOncePerFamily(t *testing.T) {
	// A scraper rejects a metric family declared twice, so samples that share a
	// name must share one header no matter how they were ordered on the way in.
	got := render(t,
		Sample{Name: "m", Help: "h", Kind: Gauge, Value: 2, Labels: []Label{{"peer", "3"}}},
		Sample{Name: "other", Kind: Gauge, Value: 1},
		Sample{Name: "m", Help: "h", Kind: Gauge, Value: 1, Labels: []Label{{"peer", "2"}}},
	)
	if n := strings.Count(got, "# TYPE m "); n != 1 {
		t.Errorf("TYPE for m appears %d times:\n%s", n, got)
	}
	if n := strings.Count(got, "# HELP m "); n != 1 {
		t.Errorf("HELP for m appears %d times:\n%s", n, got)
	}
	// And the two samples must both survive, grouped under it.
	if !strings.Contains(got, `m{peer="2"} 1`) || !strings.Contains(got, `m{peer="3"} 2`) {
		t.Errorf("a labelled sample was lost:\n%s", got)
	}
}

func TestOutputIsStableAcrossOrderings(t *testing.T) {
	// Two scrapes of an unchanged process must produce identical bytes, or the
	// output is neither diffable nor testable.
	a := render(t,
		Sample{Name: "b", Kind: Gauge, Value: 1},
		Sample{Name: "a", Kind: Counter, Value: 2, Labels: []Label{{"x", "2"}}},
		Sample{Name: "a", Kind: Counter, Value: 3, Labels: []Label{{"x", "1"}}},
	)
	b := render(t,
		Sample{Name: "a", Kind: Counter, Value: 3, Labels: []Label{{"x", "1"}}},
		Sample{Name: "b", Kind: Gauge, Value: 1},
		Sample{Name: "a", Kind: Counter, Value: 2, Labels: []Label{{"x", "2"}}},
	)
	if a != b {
		t.Errorf("the same samples rendered differently:\n%s\n---\n%s", a, b)
	}
}

func TestCountersAndGaugesAreDistinguished(t *testing.T) {
	got := render(t,
		Sample{Name: "c", Kind: Counter, Value: 1},
		Sample{Name: "g", Kind: Gauge, Value: 1},
	)
	if !strings.Contains(got, "# TYPE c counter") {
		t.Error("counter not typed as one")
	}
	if !strings.Contains(got, "# TYPE g gauge") {
		t.Error("gauge not typed as one")
	}
}

func TestWholeNumbersRenderWithoutExponents(t *testing.T) {
	// Every value here is a count or a log index. Someone curling the endpoint to
	// see a commit index should read a number, not scientific notation.
	got := render(t, Sample{Name: "quorumkv_raft_commit_index", Kind: Gauge, Value: 1234567})
	if !strings.Contains(got, "quorumkv_raft_commit_index 1234567\n") {
		t.Errorf("a whole number was reformatted:\n%s", got)
	}
}

func TestSpecialFloatsUseTheFormatsSpelling(t *testing.T) {
	for _, tc := range []struct {
		value float64
		want  string
	}{
		{math.Inf(1), "+Inf"},
		{math.Inf(-1), "-Inf"},
		{math.NaN(), "NaN"},
		{0.5, "0.5"},
	} {
		got := render(t, Sample{Name: "v", Kind: Gauge, Value: tc.value})
		if !strings.Contains(got, "v "+tc.want+"\n") {
			t.Errorf("value %v rendered as %q, want %q", tc.value, got, tc.want)
		}
	}
}

func TestLabelValuesAndHelpAreEscaped(t *testing.T) {
	// An unescaped quote or newline produces a line the scraper cannot parse, and
	// the whole scrape fails rather than that one metric.
	got := render(t, Sample{
		Name:   "m",
		Help:   "line one\nline two \\ here",
		Kind:   Gauge,
		Value:  1,
		Labels: []Label{{"addr", `host:"9000"`}, {"note", "a\nb"}},
	})
	for _, want := range []string{
		`# HELP m line one\nline two \\ here`,
		`addr="host:\"9000\""`,
		`note="a\nb"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
		if line == "" {
			t.Error("an escaped value produced a blank line, so it was not escaped")
		}
	}
}

func TestSampleWithoutHelpOmitsTheHeader(t *testing.T) {
	got := render(t, Sample{Name: "m", Kind: Gauge, Value: 1})
	if strings.Contains(got, "# HELP") {
		t.Errorf("emitted an empty HELP line:\n%s", got)
	}
	if !strings.Contains(got, "# TYPE m gauge") {
		t.Errorf("TYPE is required even without help:\n%s", got)
	}
}
