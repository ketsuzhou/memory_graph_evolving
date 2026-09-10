package retrieval

import (
	"math"
	"unicode"
)

const (
	bm25K1 = 1.5
	bm25B  = 0.75
)

type bm25Document struct {
	terms  map[string]int
	length int
}

type bm25Index struct {
	documents []bm25Document
	df        map[string]int
	averageDL float64
}

func buildBM25(contents []string) bm25Index {
	index := bm25Index{documents: make([]bm25Document, len(contents)), df: make(map[string]int)}
	var total int
	for i, content := range contents {
		tokens := tokenize(content)
		document := bm25Document{terms: make(map[string]int), length: len(tokens)}
		seen := make(map[string]struct{})
		for _, token := range tokens {
			document.terms[token]++
			if _, ok := seen[token]; !ok {
				seen[token] = struct{}{}
				index.df[token]++
			}
		}
		index.documents[i] = document
		total += document.length
	}
	if len(index.documents) > 0 {
		index.averageDL = float64(total) / float64(len(index.documents))
	}
	return index
}

// score returns Robertson BM25 scores max-normalized across this immutable
// corpus. Query terms are distinct, so repeated query words cannot inflate a
// result.
func (index bm25Index) score(query string) []float64 {
	scores := make([]float64, len(index.documents))
	if len(index.documents) == 0 || index.averageDL == 0 {
		return scores
	}
	queryTerms := make(map[string]struct{})
	for _, term := range tokenize(query) {
		queryTerms[term] = struct{}{}
	}
	if len(queryTerms) == 0 {
		return scores
	}

	corpusSize := float64(len(index.documents))
	var maximum float64
	for i, document := range index.documents {
		var score float64
		for term := range queryTerms {
			tf := document.terms[term]
			if tf == 0 {
				continue
			}
			df := float64(index.df[term])
			idf := math.Log(1 + (corpusSize-df+0.5)/(df+0.5))
			frequency := float64(tf)
			lengthNorm := 1 - bm25B + bm25B*float64(document.length)/index.averageDL
			score += idf * frequency * (bm25K1 + 1) / (frequency + bm25K1*lengthNorm)
		}
		scores[i] = score
		if score > maximum {
			maximum = score
		}
	}
	if maximum > 0 {
		for i := range scores {
			scores[i] /= maximum
		}
	}
	return scores
}

func tokenize(text string) []string {
	lower := []rune(text)
	for i := range lower {
		lower[i] = unicode.ToLower(lower[i])
	}

	tokens := make([]string, 0)
	word := make([]rune, 0)
	han := make([]rune, 0)
	flushWord := func() {
		if len(word) == 0 {
			return
		}
		token := string(word)
		if _, stopped := englishStopWords[token]; !stopped {
			tokens = append(tokens, token)
		}
		word = word[:0]
	}
	flushHan := func() {
		if len(han) == 1 {
			tokens = append(tokens, string(han))
		} else {
			for i := 0; i+1 < len(han); i++ {
				tokens = append(tokens, string(han[i:i+2]))
			}
		}
		han = han[:0]
	}

	for _, r := range lower {
		switch {
		case isCJK(r):
			flushWord()
			han = append(han, r)
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			flushHan()
			word = append(word, r)
		default:
			flushWord()
			flushHan()
		}
	}
	flushWord()
	flushHan()
	return tokens
}

func isCJK(r rune) bool {
	return unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) ||
		unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r) ||
		unicode.Is(unicode.Bopomofo, r)
}

var englishStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "been": {}, "but": {}, "by": {},
	"for": {}, "from": {}, "had": {}, "has": {}, "have": {}, "he": {}, "her": {}, "hers": {}, "him": {}, "his": {},
	"i": {}, "if": {}, "in": {}, "into": {}, "is": {}, "it": {}, "its": {}, "me": {}, "my": {}, "no": {}, "not": {},
	"of": {}, "on": {}, "or": {}, "our": {}, "ours": {}, "she": {}, "so": {}, "that": {}, "the": {}, "their": {},
	"theirs": {}, "them": {}, "they": {}, "this": {}, "those": {}, "to": {}, "too": {}, "us": {}, "was": {}, "we": {},
	"were": {}, "what": {}, "when": {}, "where": {}, "which": {}, "who": {}, "will": {}, "with": {}, "you": {}, "your": {},
	"yours": {},
}
