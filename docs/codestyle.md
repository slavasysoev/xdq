* Don't use short names, use complete: e.g. user instead of u
* Don't use comments on the same line as code
* Packages: lowercase, single word, match directory.

## Naming

* One name, one concept. If a word already means something in the project, don't reuse it for
  another thing. `source` is the adapter (`source.Source`); the name of a source in the config
  is `sourceName`.
* Types that clash with a package name get a suffix: `config.KafkaConfig`, not `config.Kafka`
  next to the `kafka` package. All structs of the config package end with `Config`.
* A variable named after its type's package is a smell. Name the variable for its role:
  `configuration` for a `*config.Config`, `sourceResult` for a `report.SourceResult`.
* A field is named for what it holds, not for its Go type: `MemberObservation.Endpoint`,
  not `Node`.
* Don't stutter: `groupState.phase`, not `groupState.state`.
* Constants of one kind share a prefix (`StatusOK`, `SeverityError`, `SASLPlain`) and have
  their own type when the values form a closed set.
* Package doc comment lives in `doc.go`, or in the file named like the package when it is the
  main file.

## Abstractions

* Separate concepts get separate types, even when the values look alike. `report.Status` is the
  traffic light of an item, `report.Severity` is how bad a finding is: a finding is never `ok`.
* Define a value once. Exit codes are CLI business and live in `internal/cli`; the report only
  answers facts (`IsComplete`, `HasErrors`). Don't copy a constant into another package.
* A constructor leaves the object valid. No "call X after you add everything": `report.New`
  computes statuses itself.
* Code that has to know every implementation (a switch on source type) lives in one place, the
  composition root (`cli/sources.go`). Everything else talks to an interface.
* Each source type implements its own validation and resolving (`config.SourceSettings`), so
  adding a type does not mean editing a switch in several places.
* An interface lists what its consumer needs, not what one implementation happens to have.
* Don't introduce a wrapper struct that only repeats fields of the thing it wraps
  (the old `engine.Input` repeated `Name` and `Type` of the source).

## Functions

* Return results, don't fill arguments. A function that takes a map "to write into" is a
  function that should return a map.
* One function, one level of detail. When a function needs section comments to be read or grows
  past ~50 lines, split it (`buildSnapshot` → `addTopics`, `addGroup`, `addMembers`).
* A pair of values returned from one function that always travel together is a small struct
  (`discovery`).
* Repeated error plumbing becomes a helper (`responseError`), after the third repetition.
* Build empty containers in one constructor (`graph.NewSnapshot`), not by copying `make` calls.

## Tooling

* Go: 1.27.4 (pinned by the `toolchain` line in [go.mod](../go.mod)).
* golangci-lint: v2.15.0. Config: [.golangci.yml](../.golangci.yml). Run: `golangci-lint run ./...`.
* Formatting: `golangci-lint fmt`.
