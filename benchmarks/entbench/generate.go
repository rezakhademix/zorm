// Package entbench holds the ent schema + (after `go generate ./...`) the
// generated client used by the ent benchmarks.
//
// Run `go generate ./entbench` from the benchmarks module root once; the
// generated client lands under ./entbench/. After that, run the ent benches with:
//
//	go test -tags=ent -bench=BenchmarkEnt -benchmem ./...
package entbench

//go:generate go run -mod=mod entgo.io/ent/cmd/ent generate ./schema
