package ethspam

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type QueryContent struct {
	Id     int64
	Method string
	Params string
}

func (q *QueryContent) GetBody() string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"%s","params":%s}`+"\n", q.Id, q.Method, q.Params)
}

func genEthCall(s State) QueryContent {
	// We eth_call the block before the call actually happened to avoid collision reverts
	to, from, input, block := s.RandomCall()
	res := QueryContent{
		Id:     s.ID(),
		Method: "eth_call",
	}
	if to != "" {
		res.Params = fmt.Sprintf(`[{"to":%q,"from":%q,"data":%q},"0x%x"]`, to, from, input, block-1)
	} else {
		res.Params = fmt.Sprintf(`[{"from":%q,"data":%q},"0x%x"]`, from, input, block-1)
	}

	return res
}

func genEthGetTransactionReceipt(s State) QueryContent {
	txID := s.RandomTransaction()
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getTransactionReceipt",
		Params: fmt.Sprintf(`["%s"]`, txID),
	}
}

func genEthGetBalance(s State) QueryContent {
	addr := s.RandomAddress()
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getBalance",
		Params: fmt.Sprintf(`["%s","latest"]`, addr),
	}
}

func genEthGetBlockByNumber(s State) QueryContent {
	r := s.RandInt64()
	blockNum := s.CurrentBlock() - uint64(r%5) // Within the last ~minute
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getBlockByNumber",
		Params: fmt.Sprintf(`["0x%x",false]`, blockNum),
	}
}

func genEthGetBlockByNumberFull(s State) QueryContent {
	r := s.RandInt64()
	blockNum := s.CurrentBlock() - uint64(r%5) // Within the last ~minute
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getBlockByNumber",
		Params: fmt.Sprintf(`["0x%x",true]`, blockNum),
	}
}

func genEthGetTransactionCount(s State) QueryContent {
	addr := s.RandomAddress()
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getTransactionCount",
		Params: fmt.Sprintf(`["%s","pending"]`, addr),
	}
}

func genEthBlockNumber(s State) QueryContent {
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_blockNumber",
		Params: "[]",
	}
}

func genEthGetTransactionByHash(s State) QueryContent {
	txID := s.RandomTransaction()
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getTransactionByHash",
		Params: fmt.Sprintf(`["%s"]`, txID),
	}
}

func genEthGetLogs(s State) QueryContent {
	r := s.RandInt64()
	// TODO: Favour latest/recent block on a curve
	fromBlock := s.CurrentBlock() - uint64(r%5000) // Pick a block within the last ~day
	toBlock := s.CurrentBlock() - uint64(r%5)      // Within the last ~minute
	address, topics := s.RandomContract()
	topicsJoined := strings.Join(topics, `","`)
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getLogs",
		Params: fmt.Sprintf(`[{"fromBlock":"0x%x","toBlock":"0x%x","address":"%s","topics":["%s"]}]`, fromBlock, toBlock, address, topicsJoined),
	}
}

func genEthGetCode(s State) QueryContent {
	addr, _ := s.RandomContract()
	return QueryContent{
		Id:     s.ID(),
		Method: "eth_getCode",
		Params: fmt.Sprintf(`["%s","latest"]`, addr),
	}
}

func MakeQueriesGenerator(methods map[string]int64) (gen QueriesGenerator, err error) {
	// Top queries by weight, pulled from a 5000 Infura query sample on Dec 2019.
	//     3 "eth_accounts"
	//     4 "eth_getStorageAt"
	//     4 "eth_syncing"
	//     7 "net_peerCount"
	//    12 "net_listening"
	//    14 "eth_gasPrice"
	//    16 "eth_sendRawTransaction"
	//    25 "net_version"
	//    30 "eth_getTransactionByBlockNumberAndIndex"
	//    38 "eth_getBlockByHash"
	//    45 "eth_estimateGas"
	//    88 "eth_getCode"
	//   252 "eth_getLogs"
	//   255 "eth_getTransactionByHash"
	//   333 "eth_blockNumber"
	//   390 "eth_getTransactionCount"
	//   399 "eth_getBlockByNumber"
	//   545 "eth_getBalance"
	//   607 "eth_getTransactionReceipt"
	//  1928 "eth_call"

	rpcMethod := map[string]func(State) QueryContent{
		"eth_call":                                genEthCall,
		"eth_getTransactionReceipt":               genEthGetTransactionReceipt,
		"eth_getBalance":                          genEthGetBalance,
		"eth_getBlockByNumber":                    genEthGetBlockByNumber,
		"eth_getBlockByNumber#full":               genEthGetBlockByNumberFull,
		"eth_getTransactionCount":                 genEthGetTransactionCount,
		"eth_blockNumber":                         genEthBlockNumber,
		"eth_getTransactionByHash":                genEthGetTransactionByHash,
		"eth_getLogs":                             genEthGetLogs,
		"eth_getCode":                             genEthGetCode,
	}

	for method, weight := range methods {
		if weight == 0 {
			continue
		}
		if _, ok := rpcMethod[method]; !ok {
			return QueriesGenerator{}, errors.New(method + " is not supported")
		}
		gen.Add(RandomQuery{
			Method:   method,
			Weight:   weight,
			Generate: rpcMethod[method],
		})
	}

	return gen, nil
}

type Generator func(State) QueryContent

type RandomQuery struct {
	Method   string
	Weight   int64
	Generate Generator
}

type QueriesGenerator struct {
	queries     []RandomQuery // sorted by weight asc
	totalWeight int64
}

// Add inserts a random query QueriesGenerator with a weighted probability. Not
// goroutine-safe, should be run once during initialization.
func (g *QueriesGenerator) Add(query RandomQuery) {
	if g.queries == nil {
		g.queries = make([]RandomQuery, 1)
	} else {
		g.queries = append(g.queries, RandomQuery{})
	}
	// Maintain weight sort
	idx := sort.Search(len(g.queries), func(i int) bool { return g.queries[i].Weight < query.Weight })
	copy(g.queries[idx+1:], g.queries[idx:])
	g.queries[idx] = query
	g.totalWeight += query.Weight
}

// Query selects a QueriesGenerator based on proportonal weighted probability and
// writes the query from the QueriesGenerator.
func (g *QueriesGenerator) Query(s State) (QueryContent, error) {
	if len(g.queries) == 0 {
		return QueryContent{}, errors.New("no query generators available")
	}

	weight := s.RandInt64() % g.totalWeight

	var current int64
	for _, q := range g.queries {
		current += q.Weight
		if current >= weight {
			return q.Generate(s), nil
		}
	}

	panic("off by one bug in weighted query selection")
}
