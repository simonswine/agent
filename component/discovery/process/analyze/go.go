package analyze

import (
	"debug/buildinfo"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/grafana/agent/component/discovery/process/analyze/procnet"
	"github.com/vishvananda/netlink"
)

const (
	LabelGo                     = "__meta_process_go__"
	LabelGoVersion              = "__meta_process_go_version__"
	LabelGoModulePath           = "__meta_process_go_module_path__"
	LabelGoModuleVersion        = "__meta_process_go_module_version__"
	LabelGoSdk                  = "__meta_process_go_sdk__"
	LabelGoSdkVersion           = "__meta_process_go_sdk_version__"
	LabelGoDeltaProf            = "__meta_process_go_godeltaprof__"
	LabelGoDeltaProfVersion     = "__meta_process_go_godeltaprof_version__"
	LabelGoDeltaProfProbePrefix = "__meta_process_go_godeltaprof_probe_"
	LabelGoBuildSettingPrefix   = "__meta_process_go_build_setting_"

	goSdkModule       = "github.com/grafana/pyroscope-go"
	godeltaprofModule = "github.com/grafana/pyroscope-go/godeltaprof"
)

func analyzeGo(pid string, reader io.ReaderAt, m map[string]string) error {
	info, err := buildinfo.Read(reader)
	if err != nil {
		return err
	}

	m[LabelGo] = "true"

	if info.GoVersion != "" {
		m[LabelGoVersion] = info.GoVersion
	}
	if info.Main.Path != "" {
		m[LabelGoModulePath] = info.Main.Path
	}
	if info.Main.Version != "" {
		m[LabelGoModuleVersion] = info.Main.Version
	}

	for _, setting := range info.Settings {
		k := sanitizeLabelName(setting.Key)
		m[LabelGoBuildSettingPrefix+k] = setting.Value
	}

	for _, dep := range info.Deps {
		switch dep.Path {
		case goSdkModule:
			m[LabelGoSdk] = "true"
			m[LabelGoSdkVersion] = dep.Version
		case godeltaprofModule:
			m[LabelGoDeltaProf] = "true"
			m[LabelGoDeltaProfVersion] = dep.Version
		default:
			//todo should we optionally/configurable include all deps?
			continue
		}
	}
	if m[LabelGoDeltaProf] == "true" && m[LabelGoSdk] != "true" {
		if err := probeGodeltaprofSockets(pid, m); err != nil {
			return err
		}
	}

	return io.EOF
}

func probeGodeltaprofSockets(pid string, m map[string]string) error {
	// read fds from /proc/pid/fd
	fdmap, err := findSocketInodes(pid)
	if err != nil {
		return err
	}
	procNetBytes, err := os.ReadFile(filepath.Join("/proc", pid, "net", "tcp"))
	if err != nil {
		return err
	}
	procnet := procnet.NewProcNet(procNetBytes, netlink.TCP_LISTEN) // todo this is per net ns? not per process
	for {
		next := procnet.Next()
		if next == nil {
			break
		}
		if _, ok := fdmap[next.Inode]; !ok {
			continue
		}

		schemes := []string{"https", "http"}
		paths := []string{
			"/debug/pprof/delta_heap",
			"/debug/pprof/delta_mutex",
			"/debug/pprof/delta_block",
			"/debug/pprof/heap",
			"/debug/pprof/mutex",
			"/debug/pprof/block",
		}
		for _, scheme := range schemes {
			for _, path := range paths {
				url := fmt.Sprintf("%s://%s:%d%s", scheme, next.LocalAddress.String(), next.LocalPort, path)
				resp, err := http.DefaultClient.Head(url)
				if err != nil {
					continue
				}
				if resp.StatusCode != http.StatusOK {
					continue
				}
				k := LabelGoDeltaProfProbePrefix + path[13:]
				m[k] = url
			}
		}
	}
	return nil
}

func findSocketInodes(pid string) (map[uint64]struct{}, error) {
	var fdmap map[uint64]struct{}
	procFdPath := filepath.Join("/proc", pid, "fd")
	procFd, err := os.ReadDir(procFdPath)
	if err != nil {
		return fdmap, err
	}
	for _, fd := range procFd {
		name := fd.Name()
		v, err := os.Readlink(filepath.Join(procFdPath, name))
		if err != nil {
			continue
		}
		if !strings.HasPrefix(v, "socket:[") && strings.HasSuffix(v, "]") {
			continue
		}
		v = v[len("socket:[") : len(v)-1]
		inode, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			continue
		}
		if fdmap == nil {
			fdmap = make(map[uint64]struct{})
		}
		fdmap[inode] = struct{}{}
	}
	return fdmap, nil
}

var sanitizeRe = regexp.MustCompile("[^a-zA-Z0-9_]")

func sanitizeLabelName(s string) string {
	s = sanitizeRe.ReplaceAllString(s, "_")
	return strings.ToLower(s)
}
