package selector

import (
	"errors"
	"github.com/DCSO/balboa/observation"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
)

/*
 * TODO:
 *   * implement reinitialize
 */

func init() {
	err := RegisterSelector("regex", MakeRegexSelector)
	if err != nil {
		panic(err)
	}
}

type RegexSelector struct {
	patterns []*regexp.Regexp
	tags     []string
	ingest   []string
	filter   bool
}

func (RegexSelector) Reinitialize() (err error) {
	panic("implement me")
}

func MakeRegexSelector(config interface{}) (selector Selector, err error) {
	r := &RegexSelector{}
	if configKV, ok := config.(map[interface{}]interface{}); ok {
		if regexListInterface, ok := configKV["regexp"]; ok {
			if regexList, ok := regexListInterface.([]interface{}); ok {
				for _, regexFile := range regexList {
					r.loadSelectors(regexFile.(string))
				}
			} else {
				return nil, errors.New("type assertion failed")
			}
		} else {
			return nil, errors.New("type assertion failed")
		}
		if tagListInterface, ok := configKV["tags"]; ok {
			if tagList, ok := tagListInterface.([]interface{}); ok {
				for _, tag := range tagList {
					r.tags = append(r.tags, tag.(string))
				}
			} else {
				return nil, errors.New("type assertion failed")
			}
		}
		if ingestListInterface, ok := configKV["ingest"]; ok {
			if ingestList, ok := ingestListInterface.([]interface{}); ok {
				for _, tag := range ingestList {
					r.ingest = append(r.ingest, tag.(string))
				}
			} else {
				return nil, errors.New("type assertion failed")
			}
		}
		// the default mode is select
		r.filter = false
		if modeOption, ok := configKV["mode"]; ok {
			if modeOption == "filter" {
				r.filter = true
			}
		}
	} else {
		return nil, errors.New("type assertion failed")
	}
	return r, nil
}

func (r *RegexSelector) IngestionList() []string {
	return r.ingest
}

func (r *RegexSelector) matchSelectors(domain string) (match bool) {
	for _, s := range r.patterns {
		if s.Match([]byte(domain)) {
			return true
		}
	}
	return false
}

func (r *RegexSelector) ProcessObservation(observation *observation.InputObservation) (err error) {
	var tag bool
	tag = r.matchSelectors(observation.Rrname)
	if r.filter {
		// only set the tag when the expression does not match
		tag = !tag
	}
	if tag {
		// set tags for this selector
		for _, t := range r.tags {
			observation.Tags[t] = struct{}{}
		}
	}
	return nil
}

func (r *RegexSelector) loadSelectors(patternFile string) {
	selectorsRaw, err := ioutil.ReadFile(patternFile)
	if err != nil {
		log.Fatalf("could not read selector file due to %v", err)
	}
	for _, s := range strings.Split(string(selectorsRaw), "\n") {
		if s == "" {
			continue
		}
		regexp := regexp.MustCompile(s)
		if regexp == nil {
			log.Fatalf("regexp %s does not compile", s)
		}
		r.patterns = append(r.patterns, regexp)
	}
}
