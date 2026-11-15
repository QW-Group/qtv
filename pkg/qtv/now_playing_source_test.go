package qtv

import "testing"

func TestNowPlayingSource(t *testing.T) {
	const (
		publicAddress = "br.quake.world:27501"
		gameSource    = "192.168.1.2:28501"
		proxySource   = "127.0.0.1:28000"
		watchAddress  = "qtv.example.net:28000"
	)
	for _, tc := range []struct {
		name     string
		source   string
		hostport string
		proxy    bool
		inactive bool
		want     string
	}{
		{name: "private upstream", source: "tcp:" + gameSource, hostport: publicAddress, want: publicAddress},
		{name: "empty source selector", source: "tcp:@" + gameSource, hostport: publicAddress, want: publicAddress},
		{name: "public upstream", source: "tcp:public.example:28501", hostport: publicAddress, want: publicAddress},
		{name: "missing hostport", source: "tcp:" + gameSource, want: gameSource},
		{name: "inactive", source: "tcp:" + gameSource, hostport: publicAddress, inactive: true, want: gameSource},
		{name: "proxy", source: "tcp:" + proxySource, hostport: publicAddress, proxy: true, want: proxySource},
		{name: "chained", source: "tcp:1@" + proxySource, hostport: publicAddress, want: "1@" + proxySource},
		{name: "remote recording", source: "tcp:file:match.mvd@" + proxySource, hostport: publicAddress, want: "match.mvd@" + proxySource},
		{name: "recording", source: "file:match.mvd", hostport: publicAddress, want: "match.mvd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			us, _ := bufferingTestStream(t, 0, false)
			us.qtv.qvs.Reg("address", watchAddress)
			us.qtv.qvs.Reg("qtv_password", "")
			us.server = tc.source
			if tc.inactive {
				us.state = usParsingConnection
			}
			updates := make(chan interface{}, 1)
			us.ussNotifyCh = updates

			serverInfo := `\hostname\` + publicAddress + " (lvus vs. zubr)\207"
			if tc.hostport != "" {
				serverInfo += `\hostport\` + tc.hostport
			}
			if tc.proxy {
				serverInfo += `\*qtv\1`
			}
			if err := us.qp.readFullServerInfo("fullserverinfo " + serverInfo); err != nil {
				t.Fatal(err)
			}
			us.updateUStreamInfo()
			got := (<-updates).(*uStreamInfo)
			if got.Server != tc.want || got.Address != watchAddress {
				t.Fatalf("listing = %q, watch address = %q, want %q and %q", got.Server, got.Address, tc.want, watchAddress)
			}
		})
	}
}
