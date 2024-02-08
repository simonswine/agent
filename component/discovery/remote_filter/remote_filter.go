package remote_filter

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	ws "github.com/gorilla/websocket"
	"github.com/prometheus/prometheus/model/labels"
	cncmodel "github.com/simonswine/grafana-agent-cnc/model"

	"github.com/grafana/agent/component"
	"github.com/grafana/agent/component/discovery"
	"github.com/grafana/agent/pkg/flow/logging/level"
)

func init() {
	component.Register(component.Registration{
		Name:    "discovery.remote_filter",
		Args:    Arguments{},
		Exports: Exports{},

		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Arguments holds values which are used to configure the discovery.remote_filter component.
type Arguments struct {
	// Targets contains the input 'targets' passed by a service discovery component.
	Targets []discovery.Target `river:"targets,attr"`

	WebsocketURL string `river:"websocket_url,attr"`
}

// Exports holds values which are exported by the discovery.remote_filter component.
type Exports struct {
	WebsocketStatus string             `river:"websocket_status,attr"`
	Output          []discovery.Target `river:"output,attr"`
	Rules           []cncmodel.Rule    `river:"rules,attr"`
}

func (e *Exports) SetToDefaults() {
	e.WebsocketStatus = "connecting"
}

// Component implements the discovery.remote_filter component.
type Component struct {
	opts     component.Options
	logger   log.Logger
	instance string

	websocket *websocket

	mut         sync.RWMutex
	rules       []cncmodel.Rule
	prevTargets []discovery.Target
}

type websocket struct {
	logger log.Logger
	comp   *Component

	c   *ws.Conn
	url string

	lck         sync.Mutex
	sendTargets bool
	hash        uint64
	hasher      xxhash.Digest

	wg   sync.WaitGroup
	done chan struct{}
	out  chan []byte
}

type payloadSubscribe struct {
	Topics []string `json:"topics"`
}

type payloadData struct {
	Rules []cncmodel.Rule `json:"rules"`
}

type rule cncmodel.Rule

func (c *Component) newWebsocket(urlString string) (*websocket, error) {
	w := &websocket{
		comp:   c,
		logger: c.logger,
		url:    urlString,
		done:   make(chan struct{}),
		out:    make(chan []byte, 16),
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}
	u.Path = filepath.Join(u.Path, "/ws/grafana-agent")
	w.logger = log.With(w.logger, "server_url", u.String())
	level.Debug(w.logger).Log("msg", "connecting to control server websocket")

	w.c, _, err = ws.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	var subscribe cncmodel.Message
	subscribe.Type = cncmodel.MessageTypeSubscribe
	subscribe.Payload = cncmodel.PayloadSubscribe{
		Topics: []string{"rules"},
	}
	msg, err := json.Marshal(&subscribe)
	w.out <- msg

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run()
	}()

	w.wg.Add(1)
	go func() {
		var msg struct {
			MsgType string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		defer w.wg.Done()
		defer close(w.done)
		for {
			err := w.c.ReadJSON(&msg)
			if err != nil {
				level.Error(w.logger).Log("msg", "error reading JSON message", "err", err)
				return
			}
			level.Debug(w.logger).Log("msg", "received message", "type", msg.MsgType, "payload", string(msg.Payload))

			switch cncmodel.MessageType(msg.MsgType) {
			case cncmodel.MessageTypeData:
				var p cncmodel.PayloadData
				if err := json.Unmarshal(msg.Payload, &p); err != nil {
					level.Error(w.logger).Log("msg", "error parsing data payload", "err", err)
					return
				}

				if len(p.Rules) == 0 {
					continue
				}

				c.updateRules(p.Rules)
			case cncmodel.MessageTypeSubscribe:
				var p cncmodel.PayloadSubscribe
				if err := json.Unmarshal(msg.Payload, &p); err != nil {
					level.Error(w.logger).Log("msg", "error parsing subscribe payload", "err", err)
					return
				}

				sendTargets := false
				for _, t := range p.Topics {
					if t == "agents" {
						level.Debug(w.logger).Log("msg", "enable sending targets to control server")
						sendTargets = true
						break
					}
				}
				w.lck.Lock()
				w.sendTargets = sendTargets
				w.lck.Unlock()
				w.comp.mut.RLock()
				targets := copyTargets(w.comp.prevTargets)
				w.comp.mut.RUnlock()
				w.publishTargets(targets)
			}
		}
	}()

	return w, nil
}

func copyTargets(disc []discovery.Target) []map[string]string {
	t := make([]map[string]string, len(disc))
	for idx, m := range disc {
		n := make(map[string]string, len(m))
		for key, value := range m {
			n[key] = value
		}
		t[idx] = n
	}
	return t
}

func (w *websocket) publishTargets(targets []map[string]string) {
	w.lck.Lock()
	defer w.lck.Unlock()

	if !w.sendTargets {
		return
	}

	if len(targets) <= 0 {
		return
	}

	var p cncmodel.PayloadData
	p.Agents = append(p.Agents, cncmodel.Agent{
		Name:        w.comp.instance,
		Targets:     targets,
		LastUpdated: time.Now(),
	})

	data, err := json.Marshal(&cncmodel.Message{
		Type:    cncmodel.MessageTypeData,
		Payload: p,
	})
	if err != nil {
		panic(err)
	}

	w.hasher.Reset()
	_, _ = w.hasher.Write(data)
	if hash := w.hasher.Sum64(); w.hash == hash {
		return
	} else {
		w.hash = hash
	}

	level.Debug(w.logger).Log("msg", "publish targets to the control server", "targets", len(targets))
	w.out <- data

}

func (w *websocket) run() error {
	for {
		select {
		case <-w.done:
			return nil
		case t := <-w.out:
			wr, err := w.c.NextWriter(ws.TextMessage)
			if err != nil {
				level.Error(w.logger).Log("msg", "error creating writer", "err", err)
				continue
			}
			_, err = wr.Write(t)
			err2 := wr.Close()
			if err != nil {
				level.Error(w.logger).Log("msg", "error writing message", "err", err)
				continue
			}
			if err2 != nil {
				level.Error(w.logger).Log("msg", "error closing writer", "err", err2)
				continue
			}

			/* TODO bring this back

			case <-interrupt:

						// Cleanly close the connection by sending a close message and then
						// waiting (with timeout) for the server to close the connection.
						err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
						if err != nil {
							return err
						}
						select {
						case <-done:
						case <-time.After(time.Second):
						}
						return nil
			*/
		}
	}
}

func (w *websocket) Close() error {
	close(w.done)
	w.wg.Wait()

	return w.c.Close()
}

var _ component.Component = (*Component)(nil)

func defaultInstance() string {
	hostname := os.Getenv("HOSTNAME")
	if hostname != "" {
		return hostname
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// New creates a new discovery.remote_filter component.
func New(o component.Options, args Arguments) (*Component, error) {
	c := &Component{
		instance: defaultInstance(),
		opts:     o,
		logger:   log.With(o.Logger, "component", "discovery.remote_filter"),
	}

	// Call to Update() to set the output once at the start
	if err := c.Update(args); err != nil {
		return nil, err
	}

	return c, nil
}

// Run implements component.Component.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *Component) updateRules(rules []cncmodel.Rule) {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.rules = rules

	targets := filterTargets(c.prevTargets, c.rules)

	c.opts.OnStateChange(Exports{
		Output: targets,
		Rules:  c.rules,
	})
}

func selectorMatches(s cncmodel.Selector, lbls labels.Labels) bool {
	for _, m := range s {
		if v := lbls.Get(m.Name); !m.Matches(v) {
			return false
		}
	}
	return true
}

func filterTargets(targets []discovery.Target, rules []cncmodel.Rule) []discovery.Target {
	var (
		result = make([]discovery.Target, 0, len(targets))
		add    bool
	)

	for _, t := range targets {
		add = false
		for _, r := range rules {
			if !selectorMatches(r.Selector, t.Labels()) {
				continue
			}
			if r.Action == cncmodel.ActionKeep {
				add = true
				break
			}
			if r.Action == cncmodel.ActionDrop {
				add = false
				break
			}
		}
		if add {
			result = append(result, t)
		}
	}

	return result
}

func (c *Component) websocketCloseHandler(code int, text string) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.websocket = nil
	return nil
}

// Update implements component.Component.
func (c *Component) Update(args component.Arguments) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	newArgs := args.(Arguments)

	// check if websocket needs replacement
	if c.websocket != nil && c.websocket.url != newArgs.WebsocketURL {
		if err := c.websocket.Close(); err != nil {
		}
		c.websocket = nil
	}

	// check if the websocket connection needs to be established
	if c.websocket == nil {
		w, err := c.newWebsocket(newArgs.WebsocketURL)
		if err != nil {
			level.Error(c.logger).Log("msg", "error creating websocket", "err", err)
			return nil
		}
		w.c.SetCloseHandler(c.websocketCloseHandler)
		c.websocket = w
	}

	c.prevTargets = newArgs.Targets
	c.websocket.publishTargets(copyTargets(newArgs.Targets))
	targets := filterTargets(newArgs.Targets, c.rules)

	c.opts.OnStateChange(Exports{
		WebsocketStatus: "connected",
		Output:          targets,
		Rules:           c.rules,
	})

	return nil

}

func componentMapToPromLabels(ls discovery.Target) labels.Labels {
	res := make([]labels.Label, 0, len(ls))
	for k, v := range ls {
		res = append(res, labels.Label{Name: k, Value: v})
	}

	return res
}

func promLabelsToComponent(ls labels.Labels) discovery.Target {
	res := make(map[string]string, len(ls))
	for _, l := range ls {
		res[l.Name] = l.Value
	}

	return res
}
