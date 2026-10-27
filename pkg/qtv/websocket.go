package qtv

import (
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
)

func (sv *httpSv) websocketHandler(w http.ResponseWriter, r *http.Request) {
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:       []string{"faketcp"},
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Err(multierror.Prefix(err, "websocketHandler:")).Msg("")
		return
	}

	nc := &wsNetConn{Conn: websocket.NetConn(sv.qtv.ctx, wsConn, websocket.MessageBinary), ws: wsConn}

	if err := sv.qtv.dss.open(nc); err != nil {
		log.Err(multierror.Prefix(err, "websocketHandler:")).Msg("")
	}
}

func (sv *httpSv) websocketMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isWebSocketUpgrade(r) {
			sv.websocketHandler(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// NetConn's Close aborts in-flight reads, so no close frame gets sent.
type wsNetConn struct {
	net.Conn
	ws   *websocket.Conn
	once sync.Once
}

func (c *wsNetConn) Close() error {
	c.once.Do(func() {
		go func() {
			c.ws.Close(websocket.StatusNormalClosure, "")
			c.Conn.Close()
		}()
	})
	return nil
}
