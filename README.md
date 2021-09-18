# QTV implemented in golang

## Features

* All features from original QTV, except for RSS and admin HTTP page
* Better code base, should be easier for others to contribute and support
* Build-in HTTP server from golang (faster, support HTTPS and HTTP/2, easier to extend)
* IPv6 support, configurable
* Logging support JSON format or pretty colored format for console, configurable

## Compiling
Recent version of golang tools required, so for average user
using docker is probably the simplest way.

HINT: by default `make` without parameters will execute
two targets: *update* and *build*, so average user will compile with recent golang packages,
but it somewhat slow if you execute make command frequently,
if you want to do it faster execute `make build`, you still need to execute `make update` at least once after you get source code.

### Compiling a Linux binary

#### Using docker (recommended, unless you have recent golang in system)
```
docker pull golang
docker run --rm -v "$PWD":/qtv-go -w /qtv-go golang:latest make
```

#### Using go from the system
```
make
```

### Compiling a Windows binary

#### Using docker
```
docker pull golang
docker run --rm -v "$PWD":/qtv-go -w /qtv-go -e GOOS=windows golang:latest make
```

## Configuring
Configuration explained in [qtv.cfg](qtv/qtv.cfg), that file executed by default.

## Command line
Everything is configured with config file [qtv.cfg](qtv/qtv.cfg).
There is no useful command line switches.

Unlike traditional quake engines if you wish to pass commands
from command line to be executed by QTV then you have to use it as is:
```
./qtv-go exec example.cfg
```
If you need to pass multiple commands then you have to use semicolon (you have to escape it from the shell):
```
./qtv-go exec example1.cfg ";" exec example2.cfg
```
