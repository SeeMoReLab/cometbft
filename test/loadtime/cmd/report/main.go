package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/test/loadtime/report"
)

var (
	db       = flag.String("database-type", "goleveldb", "the type of database holding the blockstore")
	dir      = flag.String("data-dir", "", "path to the directory containing the CometBFT databases")
	csvOut   = flag.String("csv", "", "dump the extracted latencies as raw csv for use in additional tooling")
	stateLog = flag.String("state-log", "", "optional path to node state log (committed state lines) for wall-clock latency")
)

func main() {
	flag.Parse()
	if *db == "" {
		log.Fatalf("must specify a database-type")
	}
	if *dir == "" {
		log.Fatalf("must specify a data-dir")
	}
	d := strings.TrimPrefix(*dir, "~/")
	if d != *dir {
		h, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		d = h + "/" + d
	}
	_, err := os.Stat(d)
	if err != nil {
		panic(err)
	}
	dbType := dbm.BackendType(*db)
	db, err := dbm.NewDB("blockstore", dbType, d)
	if err != nil {
		panic(err)
	}
	s := store.NewBlockStore(db)
	defer s.Close()

	var commitTimes map[int64]time.Time
	if *stateLog != "" {
		commitTimes, err = report.LoadCommitTimesFromStateLog(*stateLog)
		if err != nil {
			panic(err)
		}
	}

	rs, err := report.GenerateFromBlockStoreWithCommitTimes(s, commitTimes)
	if err != nil {
		panic(err)
	}
	if *csvOut != "" {
		cf, err := os.Create(*csvOut)
		if err != nil {
			panic(err)
		}
		w := csv.NewWriter(cf)
		err = w.WriteAll(toCSVRecords(rs.List()))
		if err != nil {
			panic(err)
		}
		return
	}

	for _, r := range rs.List() {
		fmt.Printf(""+
			"Experiment ID: %s\n\n"+
			"\tLatency Source: %s\n"+
			"\tConnections: %d\n"+
			"\tRate: %d\n"+
			"\tSize: %d\n\n"+
			"\tTotal Valid Tx: %d\n"+
			"\tTotal Negative Latencies: %d\n"+
			"\tMinimum Latency: %s\n"+
			"\tMaximum Latency: %s\n"+
			"\tAverage Latency: %s\n"+
			"\tMedian Latency (P50): %s\n"+
			"\tP90 Latency: %s\n"+
			"\tP95 Latency: %s\n"+
			"\tP99 Latency: %s\n"+
			"\tStandard Deviation: %s\n"+
			"\tMissing Commit Heights: %d\n\n",
			r.ID,
			rs.TimeSource(),
			r.Connections,
			r.Rate,
			r.Size,
			len(r.All),
			r.NegativeCount,
			r.Min,
			r.Max,
			r.Avg,
			r.P50,
			r.P90,
			r.P95,
			r.P99,
			r.StdDev,
			rs.MissingCommitTimes(),
		)
	}
	fmt.Printf("Total Invalid Tx: %d\n", rs.ErrorCount())
}

func toCSVRecords(rs []report.Report) [][]string {
	total := 0
	for _, v := range rs {
		total += len(v.All)
	}
	res := make([][]string, total+1)

	res[0] = []string{"experiment_id", "block_time", "duration_ns", "tx_hash", "connections", "rate", "size"}
	offset := 1
	for _, r := range rs {
		idStr := r.ID.String()
		connStr := strconv.FormatInt(int64(r.Connections), 10)
		rateStr := strconv.FormatInt(int64(r.Rate), 10)
		sizeStr := strconv.FormatInt(int64(r.Size), 10)
		for i, v := range r.All {
			res[offset+i] = []string{idStr, strconv.FormatInt(v.BlockTime.UnixNano(), 10), strconv.FormatInt(int64(v.Duration), 10), fmt.Sprintf("%X", v.Hash), connStr, rateStr, sizeStr}
		}
		offset += len(r.All)
	}
	return res
}
