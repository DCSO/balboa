package selector

import "github.com/DCSO/balboa/observation"

/*
 * Interface for Selector implementations.
 * The "type" field in the configuration corresponds to the TypeOf(selector).Name()
 */
type Selector interface {
	/*
	 * Reinitialize the Selector, e.g. for a SIGHUP
	 */
	Reinitialize() (err error)

	/*
	 * This function processes an observation which may include:
	 *    * setting a tag
	 */
	ProcessObservation(observation *observation.InputObservation) (err error)

	/*
	 * IngestionList returns a list of tags which the selector object ingests
	 */
	IngestionList() []string
}

/*
 * Creates a new Selector with the provided configuration.
 * If a mandatory configuration option was not provided err will be set accordingly.
 */
type MakeSelector func(config interface{}) (selector Selector, err error)
