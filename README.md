# rdfc-processor-kolibrie

An [RDF-Connect](https://github.com/rdf-connect) Rust processor that applies
[Kolibrie](https://github.com/StreamIntelligenceLab/Kolibrie) Datalog reasoning
to incoming RDF streams.

## How it works

1. Reads RDF messages from an input channel — Turtle, TriG, N-Triples or
   N-Quads, auto-detected (no content-type needed).
2. Applies Datalog rules (inline or from a file) using the Kolibrie engine.
3. Writes newly derived facts as N-Triples to an output channel.

Inference is incremental by default (facts accumulate across messages) or
time-windowed when `window_size > 0`.

## Usage

No pipeline binary or static linking needed — this processor is a `cdylib`,
loaded by the plain `rust-runner` binary at pipeline-startup time.

### 1. `owl:import` this processor's `processor.ttl` in your pipeline

There's no separate "point the runner at this processor" step — importing
`processor.ttl` (locally, or straight from a GitHub Release) is the whole
mechanism. It brings in the class, its argument shape, and its
`:abiStable`/`:artifact` declarations together; the orchestrator's generic
SHACL shape (declared once in the Rust runner's `index.ttl`) extracts the
latter into the runner's process config automatically.

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .

<> owl:imports <https://github.com/<org>/kolibrie-processor/releases/download/v0.1.0/processor.ttl> .
# (a local checkout works too: <> owl:imports </path/to/kolibrie-processor/processor.ttl> .)

<reasoner> a rdfc:KolibrieProcessor ;
    rdfc:reader    <ch-in> ;
    rdfc:writer    <ch-out> ;
    rdfc:rules     """
        @prefix ex: <http://example.org/> .
        { ?x ex:parent ?y } => { ?x ex:ancestor ?y }
        { ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
    """ .
```

At pipeline-startup time the runner picks the `:artifact` entry matching its
own compiled target triple, downloads it if `:path` is `http(s)://`
(verifying and caching by `:sha256`) or opens it directly if local, and
loads it via `abi_stable`. See `processor.ttl` for the exact rules, and
`.github/workflows/build-processor.yml` for how releases are built and
published.

### 2. Build and run

```bash
cargo build --release -p rdfc-rust-runner   # from the rdfc-rust-runner repo
./target/release/rust-runner <orchestrator-addr> <runner-uri>
```

## Parameters

| field          | type    | required | description |
|----------------|---------|----------|-------------|
| `reader`       | Reader  | yes      | Input channel carrying RDF messages (Turtle/TriG/N-Triples/N-Quads, auto-detected) |
| `writer`       | Writer  | yes      | Output channel for derived N-Triples |
| `rules`        | string  | *        | Inline N3 Datalog rules |
| `rules_file`   | anyURI  | *        | Path to a file containing N3 rules |
| `window_size`  | integer | no       | Sliding-window width in seconds (0 = no windowing) |
| `window_slide` | integer | no       | Window slide interval in seconds (0 = every message) |

`rules` or `rules_file` is required (at least one must be provided).

## N3 rule format

```n3
@prefix ex: <http://example.org/> .

{ ?x ex:parent ?y } => { ?x ex:ancestor ?y }
{ ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
```

## Windowing semantics

- **No windowing** (`window_size = 0`, default): triples accumulate forever;
  only **newly derivable** facts are emitted after each message (incremental mode).
- **Windowed** (`window_size > 0`): only triples received within the last
  `window_size` seconds are considered.  Inference re-runs after every message
  (or after `window_slide` seconds if specified).  All facts derived in the
  current window are emitted each time.
