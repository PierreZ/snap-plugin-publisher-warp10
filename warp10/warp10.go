package warp10

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/ctypes"
)

const (
	name       = "warp10"
	version    = 1
	pluginType = plugin.PublisherPluginType
)

// GTS is a representation of a Geo Time Series
// Please see http://www.warp10.io/apis/gts-input-format/
type GTS struct {
	TS     string            // Timestamp of the reading, in microseconds since the Unix Epoch
	Lat    string            // geographic coordinates of the reading
	Long   string            // geographic coordinates of the reading
	Elev   string            // elevation of the reading, in millimeters
	Name   string            // Class name
	Labels map[string]string // Comma separated list of labels, using the syntax `key=value`
	Value  string            // The value of the reading
}

// Print respects the following format:
// TS/LAT:LON/ELEV NAME{LABELS} VALUE
func (gts GTS) Print() []byte {
	return []byte(gts.TS + "/" + gts.Lat + ":" + gts.Long + "/" + gts.Elev + " " + gts.Name + "{" + gts.Labels + "}" + " " + gts.Value)
}

// Meta returns a plugin meta data
func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(name, version, pluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

//NewWarp10Publisher returns an instance of the OpenTSDB publisher
func NewWarp10Publisher() *opentsdbPublisher {
	return &warp10Publisher{}
}

type warp10Publisher struct {
}

func (p *warp10Publisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewStringRule("host", true)
	handleErr(err)
	r1.Description = "Warp10 Ingress host"
	config.Add(r1)

	r2, err := cpolicy.NewIntegerRule("token", true)
	handleErr(err)
	r2.Description = "Warp10 write token"
	config.Add(r2)

	cp.Add([]string{""}, config)
	return cp, nil
}

// Publish publishes metric data to opentsdb.
func (p *warp10Publisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	logger := log.New()
	var metrics []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&metrics); err != nil {
			logger.Printf("Error decoding GOB: error=%v content=%v", err, content)
			return err
		}
	case plugin.SnapJSONContentType:
		err := json.Unmarshal(content, &metrics)
		if err != nil {
			logger.Printf("Error decoding JSON: error=%v content=%v", err, content)
			return err
		}
	default:
		logger.Printf("Error unknown content type '%v'", contentType)
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}

	var temp GTS
	var points []GTS

	// Parsing
	for _, m := range metrics {
		tempTags := make(map[string]string)

		tags := m.Tags()
		for k, v := range tags {
			tempTags[k] = string(v)
		}
		tempTags[host] = string(tags[core.STD_TAG_PLUGIN_RUNNING_ON])

		temp = GTS{
			m.Timestamp().Unix(),
			"", // Lat
			"", // Long
			"", // Elev
			strings.Join(m.Namespace().Strings(), "."),
			tempTags,
			string(m.Data()),
		}
		pts = append(pts, temp)
	}

	// Create buffer of GTS
	var b bytes.Buffer
	for pt := range pts {
		b.Write(pt.Print() + "\n")
	}

	req, err := http.NewRequest("POST", config["host"].(ctypes.ConfigValueStr).Value+"/api/v0/update", b)
	req.Header.Set("X-Warp10-Token", config["token"].(ctypes.ConfigValueStr).Value)
	client := &http.Client{}
	resp, err := client.Do(req)
	if resp.StatusCode != 200 {
		return err
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
