package qtv

import (
	"net/http"
	"strings"

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

	nc := websocket.NetConn(sv.qtv.ctx, wsConn, websocket.MessageBinary)

	done, err := sv.qtv.dss.openWS(nc)
	if err != nil {
		log.Err(multierror.Prefix(err, "websocketHandler:")).Msg("")
		wsConn.Close(websocket.StatusInternalError, err.Error())
		return
	}

	<-done
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
