package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/qqshka/qtv-go/pkg/qtv"
	"github.com/rs/zerolog/log"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	console := startConsoleReader()
	q, err := qtv.NewQTV(context.Background(), console, os.Args[0:])
	if err != nil {
		// Avoid logging flag errors since flag already print errors on stderr.
		if !errors.Is(err, flag.ErrHelp) && !strings.HasPrefix(err.Error(), "flag provided but not defined:") {
			log.Err(err).Msg("")
		}
		os.Exit(2)
	}
	if err := q.ListenAndServe(); err != nil {
		log.Err(err).Msg("")
	}
}

func startConsoleReader() chan string {
	console := make(chan string)
	go func() {
		in := bufio.NewReader(os.Stdin)
		for {
			str, err := in.ReadString('\n')
			console <- str
			if err != nil {
				log.Err(multierror.Prefix(err, "console read:")).Msg("")
				return
			}
		}
	}()

	return console
}
