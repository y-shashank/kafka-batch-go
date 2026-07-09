package daemon

import (
	"encoding/json"
	"fmt"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func fairReadyResolver(manifest config.Manifest, cfg config.Daemon, lane string) func([]byte) (string, error) {
	return func(payload []byte) (string, error) {
		var m map[string]interface{}
		if err := json.Unmarshal(payload, &m); err != nil {
			return "", err
		}
		jobType, _ := m["job_type"].(string)
		if jobType == "" {
			return "", fmt.Errorf("missing job_type in fair payload")
		}
		rt := manifest.RuntimeFor(jobType)
		if rt == "" {
			return "", fmt.Errorf("unknown job_type %q for fair ready routing", jobType)
		}
		topic := cfg.FairReadyForRuntime(lane, rt)
		if topic == "" {
			return "", fmt.Errorf("no ready topic for runtime %s job_type %s", rt, jobType)
		}
		return topic, nil
	}
}
