module river2.dev/pi-group-chat-host

go 1.26

require (
	river2.dev/graph-memory-service/testdata/gmsview v0.0.0
	river2.dev/graph-memory-service/testdata/rawadmit v0.0.0
	river2.dev/graph-memory-service/testdata/smokefix v0.0.0
)

require river2.dev/graph-memory-service v0.0.0 // indirect

replace river2.dev/graph-memory-service/testdata/rawadmit => ./internal/diagnosisfanout/testdata/rawadmit

replace river2.dev/graph-memory-service/testdata/gmsview => ./internal/skillfence/testdata/gmsview

replace river2.dev/graph-memory-service/testdata/smokefix => ./cmd/bench-runner/testdata/smokefix

replace river2.dev/graph-memory-service => ../graph-memory-service
