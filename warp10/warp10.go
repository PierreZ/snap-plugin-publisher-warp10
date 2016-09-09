package warp10

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/intelsdi-x/snap-plugin-utilities/logger"
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
	var s string
	s = fmt.Sprintf(s, "%s/%s:%s %s{%s} %s\n", gts.TS, gts.Lat, gts.Long, gts.Elev, gts.Name, gts.getLabels(), gts.Value)
	return []byte(s)
}

// getLabels format the map into the right form
func (gts GTS) getLabels() string {

	var s string
	for key, value := range gts.Labels {

		s = s + key + "=" + value + ","
	}
	// Removing last comma
	s = strings.TrimSuffix(s, ",")
	return s
}

// Meta returns a plugin meta data
func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(name, version, pluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

//NewWarp10Publisher returns an instance of the warp10 publisher
func NewWarp10Publisher() *warp10Publisher {
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
	var metrics []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&metrics); err != nil {
			logger.LogError("Error decoding GOB: error=", err, "content=", content)
			return err
		}
	case plugin.SnapJSONContentType:
		err := json.Unmarshal(content, &metrics)
		if err != nil {
			logger.LogError("Error decoding JSON: error=", err, "content=", content)
			return err
		}
	default:
		logger.LogError("Error unknown content type ", contentType)
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}

	var temp GTS
	var pts []GTS

	// Parsing
	for _, m := range metrics {
		tempTags := make(map[string]string)

		tags := m.Tags()
		for k, v := range tags {
			tempTags[k] = string(v)
		}
		tempTags["host"] = string(tags[core.STD_TAG_PLUGIN_RUNNING_ON])

		// Copy of influxdb publisher code
		newtags := map[string]string{}
		ns := m.Namespace().Strings()

		isDynamic, indexes := m.Namespace().IsDynamic()
		if isDynamic {
			for i, j := range indexes {
				// The second return value from IsDynamic(), in this case `indexes`, is the index of
				// the dynamic element in the unmodified namespace. However, here we're deleting
				// elements, which is problematic when the number of dynamic elements in a namespace is
				// greater than 1. Therefore, we subtract i (the loop iteration) from j
				// (the original index) to compensate.
				//
				// Remove "data" from the namespace and create a tag for it
				ns = append(ns[:j-i], ns[j-i+1:]...)
				newtags[m.Namespace()[j].Name] = m.Namespace()[j].Value
			}
		}
		for k, v := range newtags {
			tempTags[k] = v
		}

		temp = GTS{
			string(m.Timestamp().Unix()),
			"", // Lat
			"", // Long
			"", // Elev
			strings.Join(ns, "."),
			tempTags,
			string(m.Data().(string)),
		}
		pts = append(pts, temp)
	}

	// Create buffer of GTS
	var b bytes.Buffer
	for _, pt := range pts {
		b.Write(pt.Print())
	}

	req, err := http.NewRequest("POST", config["host"].(ctypes.ConfigValueStr).Value+"/api/v0/update", &b)
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

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
