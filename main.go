package main

import (
	"context"
	"fmt"
	ethspam "github.com/p2p-org/ethspam/lib"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/INFURA/go-ethlibs/node"
	flags "github.com/jessevdk/go-flags"
	"golang.org/x/time/rate"
)

// Version of the binary, assigned during build.
var Version = "dev"

// Options contains the flag options
type Options struct {
	Methods      map[string]int64 `short:"m" long:"method" description:"A map from json rpc methods to their weight" default:"eth_getCode:100" default:"eth_getLogs:250" default:"eth_getTransactionByHash:250" default:"eth_blockNumber:350" default:"eth_getTransactionCount:400" default:"eth_getBlockByNumber:400" default:"eth_getBalance:550" default:"eth_getTransactionReceipt:600" default:"eth_call:2000"`
	Web3Endpoint string           `long:"rpc" description:"Ethereum JSONRPC provider, such as Infura or Cloudflare" default:"https://mainnet.infura.io/v3/af500e495f2d4e7cbcae36d0bfa66bcb"` // Versus API key on Infura
	RateLimit    float64          `short:"r" long:"ratelimit" description:"rate limit for generating jsonrpc calls"`

	Version bool `long:"version" description:"Print version and exit."`
}

func exit(code int, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(code)
}

func main() {
	options := Options{}
	p, err := flags.NewParser(&options, flags.Default).ParseArgs(os.Args[1:])
	if err != nil {
		if p == nil {
			fmt.Println(err)
		}
		return
	}

	if options.Version {
		fmt.Println(Version)
		os.Exit(0)
	}

	gen, err := ethspam.MakeQueriesGenerator(options.Methods)
	if err != nil {
		exit(1, "failed to install defaults: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := node.NewClient(ctx, options.Web3Endpoint)
	if err != nil {
		exit(1, "failed to make a new client: %s", err)
	}
	mkState := ethspam.StateProducer{
		Client: client,
	}

	// stateChannel 😂
	stateChannel := make(chan ethspam.State, 1)

	// We don't need a high quality randomness source, just for benchmark shuffling
	randSrc := rand.NewSource(time.Now().UnixNano())
	go func() {
		state := ethspam.LiveState{
			IdGen:   &ethspam.IdGenerator{},
			RandSrc: randSrc,
		}
		for {
			newState, err := mkState.Refresh(&state)
			if err != nil {
				// It can happen in some testnets that most of the blocks
				// are empty(no transaction included), don't refresh the
				// QueriesGenerator state without new inclusion.
				if err == ethspam.ErrEmptyBlock {
					select {
					case <-time.After(5 * time.Second):
					case <-ctx.Done():
						return
					}
					continue
				}
				exit(2, "failed to refresh state")
			}
			select {
			case stateChannel <- newState:
			case <-ctx.Done():
				return
			}

			select {
			case <-time.After(15 * time.Second):
			case <-ctx.Done():
			}
		}
	}()

	var rlimit *rate.Limiter
	if options.RateLimit != 0 {
		rlimit = rate.NewLimiter(rate.Limit(options.RateLimit), 10)
	}
	state := <-stateChannel

	queries := make(chan string)
	

	go func() {
		defer close(queries)
		for {
			// Update state when a new one is emitted
			select {
			case state = <-stateChannel:
			case <-ctx.Done():
				return
			default:
			}
			if rlimit != nil {
				rlimit.Wait(context.Background())
			}
			if q, err := gen.Query(state); err == io.EOF {
				return
			} else if err != nil {
				exit(2, "failed to write generated query: %s", err)
			} else {
				queries <- q.GetBody()
			}
		}
	}()

	for query := range queries {
		if _, err := fmt.Fprint(os.Stdout, query); err == io.EOF {
			return
		} else if err != nil {
			exit(2, "failed to write generated query: %s", err)
		}
	}
}
