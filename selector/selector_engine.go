package selector

import (
	"errors"
	"github.com/DCSO/balboa/observation"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

/*
 * TODO:
 *   * implement reinitialization functionality
 */

/*
 * Engine implements an engine which processes InputObservations with Selector implementations.
 */
type Engine struct {
	Selectors   map[string][]Selector
	stoppedChan chan bool
	stopChan    chan bool
	outChan     chan observation.InputObservation
}

/*
 * SelectorBase represents a helper type for config parsing
 */
type Base struct {
	Selectors []interface{} `yaml:"selectors"`
}

var _selectorLookupTable = make(map[string]MakeSelector)

func RegisterSelector(name string, selector MakeSelector) (err error) {
	if _, ok := _selectorLookupTable[name]; ok {
		return errors.New("selector already registered")
	}
	_selectorLookupTable[name] = selector
	return nil
}

/*
 * MakeSelectorEngine creates a new SelectorEngine from a SelectorEngine config file (YAML)
 */
func MakeSelectorEngine(config []byte, outChan chan observation.InputObservation) (engine *Engine, err error) {
	engine = &Engine{
		Selectors:   map[string][]Selector{},
		stoppedChan: make(chan bool),
		stopChan:    make(chan bool),
		outChan:     outChan,
	}

	var configMap Base
	err = yaml.Unmarshal(config, &configMap)
	if err != nil {
		return nil, err
	}

	for _, selectorConfig := range configMap.Selectors {
		if _, ok := selectorConfig.(map[interface{}]interface{}); !ok {
			return nil, errors.New("config has unexpected format")
		}
		selectorConfigTyped := selectorConfig.(map[interface{}]interface{})
		if t, ok := selectorConfigTyped["type"]; ok {
			if typeString, ok := t.(string); ok {
				if f, ok := _selectorLookupTable[typeString]; ok {
					s, err := f(selectorConfigTyped)
					if err != nil {
						return nil, err
					}
					ingestedTags := s.IngestionList()
					if len(ingestedTags) == 0 {
						// ingest option is empty, thus this selector receives all unprocessed observations
						engine.Selectors[""] = append(engine.Selectors[""], s)
					}
					for _, tag := range ingestedTags {
						engine.Selectors[tag] = append(engine.Selectors[tag], s)
					}
				} else {
					return nil, errors.New("configuration contains unknown selector type")
				}
			} else {
				return nil, errors.New("config has unexpected format")
			}
		} else {
			return nil, errors.New("could not parse selector config")
		}
	}

	return engine, nil
}

func (e *Engine) hasVisited(ob *observation.InputObservation, s Selector) (visited bool) {
	visited = false
	if _, ok := ob.Selectors[s]; ok {
		visited = true
	}
	return visited
}

/*
 * ProcessObservation processes the given observation which each applicable selector.
 * Applicable selectors are chosen based on the tags an observation already has.
 * Each selector instance is only applied once to prevent loops!
 * This has to be taken into consideration when designing a selector stack.
 *
 * ProcessObservation returns the number of applied selectors (n).
 * When n is 0 no further calls to ProcessObservation for a given selector are required.
 */
func (e *Engine) ProcessObservation(ob *observation.InputObservation) (n uint) {
	n = 0
	for tag := range ob.Tags {
		if selectors, ok := e.Selectors[tag]; ok {
			for _, s := range selectors {
				if e.hasVisited(ob, s) {
					continue
				}
				err := s.ProcessObservation(ob)
				ob.Selectors[s] = struct{}{}
				n = n + 1
				if err != nil {
					log.Errorf("error %v processing observation", err)
				}
			}
		}
	}
	return n
}

/*
 * ConsumeFeedWorker will check observations against every selector.
 * If a selector returns drop == true the observation is dropped.
 * Otherwise the observation is returned in outChan.
 *
 */
func (e *Engine) ConsumeFeedWorker(inChan chan observation.InputObservation) {
	for {
		select {
		case <-e.stopChan:
			close(e.stoppedChan)
			return
		case o := <-inChan:
			var n uint = 0
			for _, s := range e.Selectors[""] {
				var err error
				err = s.ProcessObservation(&o)
				n = n + 1
				if err != nil {
					log.Errorf("error %v processing observation", err)
				}
			}
			for n != 0 {
				n = e.ProcessObservation(&o)
			}
			o.Selectors = nil
			e.outChan <- o
		}
	}
}

func (e *Engine) ConsumeFeed(inChan chan observation.InputObservation) {
	go e.ConsumeFeedWorker(inChan)
}

/*
 * Blocking shutdown of the SelectorEngine
 */
func (e *Engine) Shutdown() {
	close(e.stopChan)
	<-e.stoppedChan
}
