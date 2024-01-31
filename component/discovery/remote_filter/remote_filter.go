package remote_filter

import (
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"sync"

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
	Output []discovery.Target `river:"output,attr"`
	Rules  []cncmodel.Rule    `river:"rules,attr"`
}

// Component implements the discovery.remote_filter component.
type Component struct {
	opts   component.Options
	logger log.Logger

	websocket *websocket

	mut         sync.RWMutex
	rules       []cncmodel.Rule
	prevTargets []discovery.Target
}

type websocket struct {
	logger log.Logger

	c   *ws.Conn
	url string

	lck         sync.Mutex
	sendTargets bool

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
		url:  urlString,
		done: make(chan struct{}),
		out:  make(chan []byte, 16),
	}

	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}
	u.Path = filepath.Join(u.Path, "/ws/grafana-agent")
	w.logger = log.With(c.logger, "server_url", u.String())
	level.Debug(w.logger).Log("msg", "connecting to control server websocket")

	w.c, _, err = ws.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	var subscribe struct {
		MsgType string `json:"type"`
		Payload struct {
			Topics []string `json:"topics"`
		} `json:"payload"`
	}
	subscribe.MsgType = "subscribe"
	subscribe.Payload.Topics = []string{"rules"}

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
			_, message, err := w.c.ReadMessage()
			if err != nil {
				level.Warn(w.logger).Log("msg", "read failed", "err", err)
				return
			}

			if err := json.Unmarshal(message, &msg); err != nil {
				level.Error(w.logger).Log("msg", "error parsing received message", "err", err)
				return
			}

			switch msg.MsgType {
			case "data":
				var p payloadData
				if err := json.Unmarshal(msg.Payload, &p); err != nil {
					level.Error(w.logger).Log("msg", "error parsing data payload", "err", err)
					return
				}

				if len(p.Rules) == 0 {
					continue
				}

				c.updateRules(p.Rules)
			case "subscribe":
				var p payloadSubscribe
				if err := json.Unmarshal(msg.Payload, &p); err != nil {
					level.Error(w.logger).Log("msg", "error parsing subscribe payload", "err", err)
					return
				}

				sendTargets := false
				for _, t := range p.Topics {
					if t == "targets" {
						level.Debug(w.logger).Log("msg", "enable sending targets to control server")
						sendTargets = true
						break
					}
				}
				w.lck.Lock()
				w.sendTargets = sendTargets
				w.lck.Unlock()
			}
		}
	}()

	return w, nil
}

func (w *websocket) run() error {
	for {
		select {
		case <-w.done:
			return nil
		case t := <-w.out:
			err := w.c.WriteMessage(ws.TextMessage, t)
			if err != nil {
				return err
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

// New creates a new discovery.remote_filter component.
func New(o component.Options, args Arguments) (*Component, error) {
	c := &Component{
		opts:   o,
		logger: log.With(o.Logger, "component", "discovery.remote_filter"),
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
		add = true
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
			return err
		}
		c.websocket = w
	}

	c.prevTargets = c.prevTargets
	targets := filterTargets(newArgs.Targets, c.rules)

	c.opts.OnStateChange(Exports{
		Output: targets,
		Rules:  c.rules,
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
