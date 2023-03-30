package testing

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/grafana/agent/cmd/internal/flowmode"
	"github.com/stretchr/testify/require"
)

const scrapeMyProfiles = `logging {
	level  = "debug"
	format = "logfmt"
}

phlare.scrape "default" {
	targets = [{
		"__address__" = "localhost:12345",
	}]
	forward_to = [phlare.write.default.receiver]
}

phlare.write "default" {
	endpoint {
		url     = "%s"
	}
}
`

func TestHelloWorld(t *testing.T) {
	var (
		switchOver     = func() {}
		switchOverOnce sync.Once
		finished       = make(chan struct{})
	)

	oldTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Log("oldTarget received push")
		switchOver()

		w.Header().Add("Content-Type", "application/json")
		fmt.Fprintln(w, "{}")
	}))
	defer oldTarget.Close()

	newTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(finished)

		w.Header().Add("Content-Type", "application/json")
		fmt.Fprintln(w, "{}")
	}))
	defer newTarget.Close()

	// write first config
	f, err := os.CreateTemp("", "phlare.river")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = f.Write([]byte(fmt.Sprintf(scrapeMyProfiles, oldTarget.URL)))
	require.NoError(t, err)

	require.NoError(t, f.Close())

	// define switch over two second config logic
	switchOver = func() {
		switchOverOnce.Do(func() {
			t.Log("switching over to new target")

			// rewrite config
			f, err := os.OpenFile(f.Name(), os.O_WRONLY|os.O_TRUNC, 0)
			require.NoError(t, err)
			_, err = f.Write([]byte(fmt.Sprintf(scrapeMyProfiles, newTarget.URL)))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			// triger reload
			resp, err := http.DefaultClient.Post("http://localhost:12345/-/reload", "text/plain", nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

		})
	}

	os.Args = []string{"flow", "run", f.Name()}
	go func() {
		flowmode.Run()
	}()

	<-finished

}
