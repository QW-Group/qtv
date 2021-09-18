package qtv

import (
	"html/template"
	"io/fs"
	stdlog "log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
)

//
// Built-in HTTP interface.
//

type httpSv struct {
	qtv                *QTV               // Parent object.
	mainTemplate       *template.Template // Base html template.
	demosTemplate      *template.Template // Demos html template, derived from the base.
	nowPlayingTemplate *template.Template // Now playing html template, derived from the base.
}

func newHttpSv(qtv *QTV) *httpSv {
	sv := &httpSv{
		qtv: qtv,
	}

	sv.regVars(qtv)

	return sv
}

func (sv *httpSv) regVars(qtv *QTV) {
	qtv.qvs.RegEx("http_enabled", "1", qVarFlagInitOnly, nil)
	qtv.qvs.RegEx("http_readtimeout", "15", qVarFlagInitOnly, nil)
	qtv.qvs.RegEx("http_writetimeout", "600", qVarFlagInitOnly, nil)
	qtv.qvs.RegEx("http_idletimeout", "60", qVarFlagInitOnly, nil)
	qtv.qvs.RegEx("http_server_cert_file", "", qVarFlagInitOnly, nil)
	qtv.qvs.RegEx("http_server_key_file", "", qVarFlagInitOnly, nil)
}

func (sv *httpSv) isEnabled() bool {
	return sv.qtv.qvs.Get("http_enabled").Bool
}

// Limit is up to 60 seconds.
func (sv *httpSv) readTimeOut() time.Duration {
	return durationBound(1, sv.qtv.qvs.Get("http_readtimeout").Dur, 60) * time.Second
}

// Limit is up to 15 minutes.
func (sv *httpSv) writeTimeOut() time.Duration {
	return durationBound(1, sv.qtv.qvs.Get("http_writetimeout").Dur, 60*15) * time.Second
}

// Limit is up to 60 seconds.
func (sv *httpSv) idleTimeOut() time.Duration {
	return durationBound(1, sv.qtv.qvs.Get("http_idletimeout").Dur, 60) * time.Second
}

type mainTemplateData struct {
	Title      string
	HelpURL    string
	ProjectURL string
	Version    string
	Build      string
	HostName   string // QTV hostname.
	Address    string
}

type demosTemplateData struct {
	mainTemplateData
	List demoList
}

type nowPlayingTemplateData struct {
	mainTemplateData
	List []*uStreamInfo
}

// Get base data required for main template.
func (sv *httpSv) getMainTemplateData(r *http.Request, title string) mainTemplateData {
	data := mainTemplateData{
		Title:      "QuakeTV: " + title,
		HelpURL:    qtvHelpURL,
		ProjectURL: qtvProjectURL,
		Version:    qtvRelease,
		Build:      qtvBuild,
		HostName:   sv.qtv.hostName(),
		Address:    sv.qtv.qvs.Get("address").Str,
	}
	if data.Address == "" {
		data.Address = r.Host
	}
	return data
}

// Convert bytes to kilobytes.
func toKb(v int64) int64 {
	return v / 1024
}

// Helps to mark rows as even/odd during html generation.
func isEven(i int) string {
	if (i & 1) != 0 {
		return "odd"
	}
	return "even"
}

var (
	// Provide our custom functions for HTML template processor.
	qtvTemplateFuncs = template.FuncMap{
		"toKb":      toKb,
		"isEven":    isEven,
		"hasSuffix": strings.HasSuffix,
	}
)

// Prepare HTTP server (parse HTML templates).
func (sv *httpSv) prepare() (err error) {
	defer func() { err = multierror.Prefix(err, "httpSv.prepare:") }()

	qtvMain := `
<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.0 Transitional//EN' 'http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'>
<html xmlns='http://www.w3.org/1999/xhtml' xml:lang='en' lang='en'>
	<head>
		<meta http-equiv="content-type" content="text/html; charset=iso-8859-1" />
		<title>{{.Title}}</title>
		<link rel="StyleSheet" href="/style.css" type="text/css" />
		<script src="/script.js" type="text/javascript"></script>
	</head>
	<body>
		<div id="navigation">
			<span><a href="/nowplaying/">Live</a></span><span><a href="/demolist/">Demos</a></span><span><span><a href="{{.HelpURL}}" target="_blank">Help</a></span>
		</div>
		{{block "qtvBody" .}}{{end}}
		<p id='version'><strong><a href="{{.ProjectURL}}">QTVGO</a> {{.Version}}, build {{.Build}}</strong></p>
	</body>
</html>
`
	sv.mainTemplate, err = template.New("qtvMain").Funcs(qtvTemplateFuncs).Parse(qtvMain)
	if err != nil {
		return err
	}

	qtvDemos := `
{{define "qtvBody"}}
<h1>QuakeTV: Demo Listing</h1>
<table id="demos" cellspacing="0">
	<thead>
		<tr>
			<th class="stream">Stream</th>
			<th class="save">Download</th>
			<th class="name">Demoname</th>
			<th class="size">Size</th>
		</tr>
	</thead>
	<tbody>
		{{range $i, $e := .List}}
		<tr class="{{isEven $i}}">
			<td class="stream">
				{{if hasSuffix .Name ".mvd"}}
					<a href="qw://file:{{.Name}}@{{$.Address}}/qtvplay"><img src="/stream.png" width="14" height="15" /></a>
				{{end}}
			</td>
			<td class="save"><a href="/demos/{{.Name}}"><img src="/save.png" width="16" height="16" /></a></td>
			<td class="name">{{.Name}}</td>
			<td class="size">{{toKb .Size}} kB</td>
		</tr>
		{{end}}
	</tbody>
</table>
<p>Total: {{len .List}} demos</p>
{{end}}
`

	sv.demosTemplate, err = sv.mainTemplate.Clone()
	if err != nil {
		return err
	}
	_, err = sv.demosTemplate.Parse(qtvDemos)
	if err != nil {
		return err
	}

	qtvNowPlaying := `
{{define "qtvBody"}}
<h1>QuakeTV: Now Playing on {{.HostName}}</h1>

<table id="nowplaying" cellspacing="0">
{{range $i, $e := .List}}
	<tr class="{{isEven $i}}{{if .WithPlayers}} notempty netop{{end}}">

		{{/* 1st cell: WATCH NOW button */}}
		{{$address := .Address}}{{if not $address}}{{$address = $.Address}}{{end}}
		<td class="wn"><span class="qtvfile"><a href="qw://{{.Id}}@{{$address}}/qtvplay">Watch&nbsp;now!</a></span></td>

		{{/* 2nd cell: server address */}}
		<td class="adr"><p class="hostname" style="display:none">{{.SvInfoHostName}}</p>{{.Server}}</td>

		{{/* 3rd cell: map name */}}
		<td class="mn">
			{{if .UpstreamStatus}}{{.UpstreamStatus}}<br />{{end}}
			{{if .MapName}}<span>{{.MapNameLong}}</span> ({{.MapName}}){{end}}
			{{if .Protected}}<br />(password protected){{end}}
		</td>
	</tr>

	{{/* Details if server not empty */}}
	{{if .WithPlayers}}
	<tr class="notempty nebottom">
		<td class="mappic">
			<img src="/levelshots/{{.MapName}}.jpg" width="144" height="108" alt="{{.MapName}}" title="{{.MapName}}" />
		</td>
		<td class="svstatus" colspan="2">
			{{template "qtvTeams" .Teams}}
			{{if .MatchStatus}}<p class="status">{{.MatchStatus}}</p>{{end}}
			<p class="observers">Observers: <span>{{len .Ds}}</span></p>
		</td>
	</tr>
	{{end}}
{{end}}
</table>

{{if not (len .List)}}
	<p>No streams are currently being played</p>
{{end}}

{{end}}

{{define "qtvTeams"}}
	{{if (gt (len .) 1)}}
		{{/* teamplay */}}
		<table class="overallscores">
			<tr class="teaminfo">
				{{range .}}
				<td>
					<span>Team: </span><span class="teamname">{{.Name}}</span>
					<span class="frags">[{{.Score}}]</span>
				</td>
				{{end}}
			</tr>
			<tr>
				{{range .}}
				<td>
					{{template "qtvOneTeam" .}}
				</td>
				{{end}}
			</tr>
		</table>
	{{else if (eq (len .) 1)}}
		{{/* non teamplay */}}
		{{template "qtvOneTeam" index . 0}}
	{{end}}
{{end}}

{{define "qtvOneTeam"}}
<table class="scores" cellspacing="0">
	<tr>
		<th>Frags</th>
		<th>Players</th>
	</tr>
	{{range $i, $e := .Players}}
	<tr class="sc{{isEven $i}}">
		<td class="frags">{{.Score}}</td>
		<td class="nick">{{.Name}}</td>
	</tr>
	{{end}}
</table>
{{end}}
`

	sv.nowPlayingTemplate, err = sv.mainTemplate.Clone()
	if err != nil {
		return err
	}
	_, err = sv.nowPlayingTemplate.Parse(qtvNowPlaying)
	if err != nil {
		return err
	}

	return nil
}

func (sv *httpSv) demosHandler(w http.ResponseWriter, r *http.Request) {
	data := demosTemplateData{
		mainTemplateData: sv.getMainTemplateData(r, "Demos"),
		List:             sv.qtv.getDemoList(),
	}

	if err := sv.demosTemplate.Execute(w, data); err != nil {
		log.Debug().Err(multierror.Prefix(err, "httpSv.demosHandler:")).Str("ctx", "httpSv").Msg("")
	}
}

func (sv *httpSv) nowPlayingHandler(w http.ResponseWriter, r *http.Request) {
	data := nowPlayingTemplateData{
		mainTemplateData: sv.getMainTemplateData(r, "Now Playing"),
		List:             sv.qtv.uss.getUStreamInfo(),
	}
	// Sort stream list by id so page looks similar on each load.
	sort.Slice(data.List, func(i, j int) bool { return data.List[i].Id < data.List[j].Id })

	if err := sv.nowPlayingTemplate.Execute(w, data); err != nil {
		log.Debug().Err(multierror.Prefix(err, "httpSv.nowHandler:")).Str("ctx", "httpSv").Msg("")
	}
}

// Returns true if file name starts with dot.
func hiddenFile(name string) bool {
	return strings.HasPrefix(name, ".")
}

// containsHiddenFile reports whether name contains a path element starting with a period.
// The name is assumed to be a delimited by forward slashes, as guaranteed
// by the http.FileSystem interface.
func containsHiddenFile(name string) bool {
	parts := strings.Split(name, "/")
	for _, part := range parts {
		if hiddenFile(part) {
			return true
		}
	}
	return false
}

// hidingFile is the http.File use in fileHidingFileSystem.
// It is used to wrap the Readdir method of http.File so that we can
// remove files and directories that start with a period from its output.
type hidingFile struct {
	http.File
}

// Readdir is a wrapper around the Readdir method of the embedded File
// that filters out all files that start with a period in their name.
func (f hidingFile) Readdir(n int) (fis []fs.FileInfo, err error) {
	files, err := f.File.Readdir(n)
	// Filters out not allowed files.
	for _, file := range files {
		name := file.Name()
		if hiddenFile(name) || fileNameHasSensitiveExtension(name) {
			continue
		}

		fis = append(fis, file)
	}
	return
}

// fileHidingFileSystem is an http.FileSystem that hides
// hidden/sensitive files from being served.
type fileHidingFileSystem struct {
	http.FileSystem
}

// Open is a wrapper around the Open method of the embedded FileSystem
// that serves a 403 permission error when name has a file or directory
// with whose name starts with a period in its path.
func (fsys fileHidingFileSystem) Open(name string) (http.File, error) {
	// If sensitive file, return 403 response
	if containsHiddenFile(name) || fileNameHasSensitiveExtension(name) {
		return nil, fs.ErrPermission
	}

	file, err := fsys.FileSystem.Open(name)
	if err != nil {
		return nil, err
	}
	return hidingFile{file}, err
}

// Serve HTTP(s) requests.
func (sv *httpSv) serve(l net.Listener) (err error) {
	r := mux.NewRouter()

	r.Handle("/", http.RedirectHandler("/nowplaying/", http.StatusMovedPermanently))

	r.HandleFunc("/nowplaying/", sv.nowPlayingHandler)
	r.HandleFunc("/demolist/", sv.demosHandler)

	// File server for demo dir.
	demosFileSys := fileHidingFileSystem{http.Dir(sv.qtv.demoDir())}
	r.PathPrefix("/demos/").Handler(http.StripPrefix("/demos/", http.FileServer(demosFileSys)))

	// File serer for qtv dir.
	// Would be better to have such files inside qtv/httproot but for backward compatibility we host whole qtv directory.
	// We hide .cfg and .dot files though.
	qtvFileSys := fileHidingFileSystem{http.Dir("qtv")}
	r.PathPrefix("/").Handler(http.FileServer(qtvFileSys))

	// Replace stdlog with zerolog inside http server.
	stdLog := stdlog.New(nil, "", 0)
	stdLog.SetFlags(0)
	stdLog.SetOutput(log.Logger)

	s := &http.Server{
		Handler:  r,
		ErrorLog: stdLog,
		// It is overall timeout for write,
		// should be quite huge so client with slow connection has a chance to download data.
		WriteTimeout: sv.writeTimeOut(),
		ReadTimeout:  sv.readTimeOut(),
		IdleTimeout:  sv.idleTimeOut(),
	}
	certFile := sv.qtv.qvs.Get("http_server_cert_file").Str
	keyFile := sv.qtv.qvs.Get("http_server_key_file").Str
	isTls := certFile != "" && keyFile != ""
	if isTls {
		err = s.ServeTLS(l, certFile, keyFile)
	} else {
		err = s.Serve(l)
	}
	// Ensure QTV is stopping if HTTP server got error of some kind.
	// This mostly required for the case when ServeTLS() could not find cert/key file.
	if err != cmux.ErrServerClosed {
		sv.qtv.Stop()
	}
	return err
}
