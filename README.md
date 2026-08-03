# rdfc-processor-kolibrie

An [RDF-Connect](https://github.com/rdf-connect) Rust processor that applies
[Kolibrie](https://github.com/StreamIntelligenceLab/Kolibrie) Datalog reasoning
and RSP-QL stream windowing to incoming RDF streams.

## How it works

1. Reads RDF messages from one or more input channels — Turtle, TriG, N-Triples
   or N-Quads, auto-detected (no content-type needed).
2. Feeds each channel into Kolibrie's native RSP-QL streaming engine as its own
   named window (`rsp_query`) — this is how multiple streams running at
   different velocities are supported: each window gets its own `RANGE`/`STEP`.
   An optional `static_reader` channel supplies permanent background data,
   joined into every window firing.
3. Applies Datalog rules (inline or from a file) by forward-chaining
   materialisation on every window firing.
4. Writes the resulting facts as N-Triples to an output channel.

A separate, optional `capped_reader` channel keeps only the last N messages
per RDF subject and reasons over exactly that retained set — see
[Per-subject capped retention](#per-subject-capped-retention-capped_reader)
below.

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
    rdfc:reader    <ch-high> , <ch-medium> ;
    rdfc:writer    <ch-out> ;
    rdfc:rspQuery  """
        REGISTER RSTREAM <http://out/stream> AS
        SELECT *
        FROM NAMED WINDOW :highWin ON <ch-high> [RANGE 5 STEP 5]
        FROM NAMED WINDOW :medWin ON <ch-medium> [RANGE 60 STEP 60]
        WHERE {
            WINDOW :highWin { ?s ?p ?o }
            WINDOW :medWin { ?s2 ?p2 ?o2 }
        }
    """ ;
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

| field              | type     | required | description |
|--------------------|----------|----------|-------------|
| `readers`          | Reader[] | yes      | One channel per RSP-QL stream (Turtle/TriG/N-Triples/N-Quads, auto-detected). Each `Reader`'s own channel URI is matched against a `FROM NAMED WINDOW ... ON <stream-iri>` clause in `rsp_query`. |
| `writer`           | Writer   | yes      | Output channel for derived N-Triples. |
| `rsp_query`        | string   | *        | Inline RSP-QL query text (required unless `rsp_query_file` is given). |
| `rsp_query_file`   | anyURI   | *        | Path to a file containing the RSP-QL query text. |
| `rules`            | string   | no       | Inline N3 Datalog rules. |
| `rules_file`       | anyURI   | no       | Path to a file containing N3 Datalog rules. |
| `sync_policy`      | string   | no       | Multi-window sync policy: `"wait"` (default) or `"steal"`. Overridden per-window by `WITH POLICY` in `rsp_query`. |
| `static_reader`    | Reader   | no       | Channel of permanent background data, accumulated forever and joined into every window firing. |
| `capped_reader`    | Reader   | no       | Separate stream that keeps only the last `max_per_subject` messages per RDF subject (see below). |
| `max_per_subject`  | integer  | *        | Required (> 0) when `capped_reader` is set. |

`rsp_query` or `rsp_query_file` is required (at least one must be provided).

## RSP-QL query format — multiple streams at different velocities

Each input stream gets its own named window with its own `RANGE`/`STEP`, and
windows are joined together (and with any static data) by shared variables in
the `WHERE` clause:

```text
REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :highWin ON <http://example.org/highVelocity> [RANGE 5 STEP 5]
FROM NAMED WINDOW :medWin ON <http://example.org/mediumVelocity> [RANGE 60 STEP 60]
WHERE {
    WINDOW :highWin { ?s ?p ?o }
    WINDOW :medWin { ?s2 ?p2 ?o2 }
}
```

- `RANGE`/`STEP` are in **seconds** (a bare integer, or an ISO-8601-style
  `PT<N>S`/`PT<N>M`/`PT<N>H` duration) — this is exactly how streams at
  different velocities get different window widths.
- Name the RDF-Connect channel in `readers` the same as the stream IRI used in
  the query's `ON <stream-iri>` clause (matched via the channel's own URI).
- For the processor to reconstruct N-Triples output, each `WINDOW` block (and
  any static pattern) that should contribute emitted triples must bind
  variables literally named `?s ?p ?o` — rows missing any of the three are
  ignored. Joins with `static_reader` data can still use other shared variable
  names, e.g. `?s ex:hasSensor ?sensor` in a window joined against
  `?sensor ex:location ?loc` outside any `WINDOW` block.
- A default `sync_policy` (`"wait"` or `"steal"`) governs what happens when
  windows fire at different rates; override it per-window with `WITH POLICY`
  in the query itself.

## N3 rule format

```n3
@prefix ex: <http://example.org/> .

{ ?x ex:parent ?y } => { ?x ex:ancestor ?y }
{ ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
```

Rules are loaded once at startup and applied via forward-chaining
materialisation on every window firing (and, independently, on the
`capped_reader` path below, if configured).

## Per-subject capped retention (`capped_reader`)

Kolibrie's RSP-QL windows (`CSPARQLWindow`) only evict by time range — there is
no API to forcibly evict a specific already-admitted item early — so "keep
only the last N messages per subject" cannot be expressed as an RSP-QL window.
When `capped_reader`/`max_per_subject` are set, this processor instead
maintains the last `max_per_subject` messages per RDF subject itself and
re-runs the same Datalog rules over exactly that retained set whenever it
changes.

**This path reasons independently of the RSP-QL engine**: its output is not
joined with the windowed/static streams above. Both paths write to the same
`writer`, and both emit an append-only stream of newly-derived facts — a fact
already emitted is never retracted, even after its source triple is later
evicted from the retained set. Joining a count-capped stream into the same
RSP-QL query would require Kolibrie itself to support that concept; not
attempted here.
