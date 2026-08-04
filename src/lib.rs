//! Kolibrie reasoning processor for RDF-Connect.
//!
//! Reads RDF messages (Turtle, TriG, N-Triples or N-Quads — auto-detected,
//! no content-type needed) from one or more `readers`, feeds them into
//! Kolibrie's native RSP-QL streaming engine (named windows bound to stream
//! IRIs, each with its own width/slide — this is how multiple input streams
//! at different velocities are supported), optionally joins in permanent
//! `static_reader` background data, applies Datalog rules (supplied inline or
//! from a file) via forward-chaining materialisation on every window firing,
//! and writes the resulting facts as N-Triples to `writer`.
//!
//! ## Parameters
//!
//! | field              | type       | required | description                                                        |
//! |---------------------|------------|----------|--------------------------------------------------------------------|
//! | `readers`           | Reader[]   | yes      | One channel per RSP-QL stream IRI (matched via each Reader's own channel URI) |
//! | `writer`            | Writer     | yes      | Channel to which derived N-Triples are written                     |
//! | `rsp_query`         | string     | *        | Inline RSP-QL query text (required unless `rsp_query_file` is given) |
//! | `rsp_query_file`    | string     | *        | Path to a file containing the RSP-QL query text                    |
//! | `rules`             | string     | no       | Inline N3 Datalog rules                                            |
//! | `rules_file`        | string     | no       | Path to a file containing N3 Datalog rules                         |
//! | `sync_policy`       | string     | no       | Multi-window sync policy: `"wait"` (default) or `"steal"`          |
//! | `static_reader`     | Reader     | no       | Channel of permanent background data, accumulated forever and joined into every window firing |
//! | `capped_reader`     | Reader     | no       | Separate stream that keeps only the last `max_per_subject` messages per RDF subject (see below) |
//! | `max_per_subject`   | integer    | *        | Required (> 0) when `capped_reader` is set                         |
//! | `subject_timeout`   | integer    | no       | Seconds of inactivity after which a capped_reader subject is evicted entirely (bounds memory; requires `capped_reader`) |
//! | `silence_event`     | string     | no       | N-Triples template (`{subject}` placeholder) injected when `subject_timeout` fires (requires `subject_timeout`) |
//! | `capped_debounce_ms`| integer    | no       | Coalesce capped_reader reasoning passes: wait this many ms after the last change before re-running (requires `capped_reader`; omit for immediate/fastest response) |
//!
//! `rsp_query` or `rsp_query_file` is required (at least one must be provided).
//!
//! ## RSP-QL query format
//!
//! Each input stream gets its own named window, with its own `RANGE`/`STEP`
//! (this is how different streams run at different velocities), joined
//! together (and with any static data) by shared variables in the `WHERE`
//! clause:
//!
//! ```text
//! REGISTER RSTREAM <http://out/stream> AS
//! SELECT *
//! FROM NAMED WINDOW :highWin ON <http://example.org/highVelocity> [RANGE 5 STEP 5]
//! FROM NAMED WINDOW :medWin ON <http://example.org/mediumVelocity> [RANGE 60 STEP 60]
//! WHERE {
//!     WINDOW :highWin { ?s ?p ?o }
//!     WINDOW :medWin { ?s2 ?p2 ?o2 }
//! }
//! ```
//!
//! Each `Reader` in `readers` is matched to a window's `ON <stream-iri>` by
//! the reader's own channel URI — name the RDF-Connect channel the same as
//! the stream IRI used in the query.
//!
//! For the processor to be able to reconstruct N-Triples output, each
//! `WINDOW` block (and any static, non-`WINDOW` pattern) that should
//! contribute to the emitted triples must bind variables literally named
//! `?s ?p ?o` (rows lacking all three are ignored). Static/background data
//! from `static_reader` can still be joined in via other shared variable
//! names (e.g. `?s ex:hasSensor ?sensor` in a window joined against
//! `?sensor ex:location ?loc` outside any `WINDOW` block).
//!
//! ## N3 rule format
//!
//! ```text
//! @prefix ex: <http://example.org/> .
//!
//! { ?x ex:parent ?y } => { ?x ex:ancestor ?y }
//! { ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
//! ```
//!
//! Rules are loaded once at startup and applied via forward-chaining
//! materialisation on every window firing (and, independently, in the
//! `capped_reader` path below).
//!
//! **Known upstream limitation — constant objects in a rule premise aren't
//! actually checked.** A premise like `?v ex:inZone ex:MarineProtectedArea`
//! is matched by Kolibrie's semi-naive join as if it were `?v ex:inZone ?_`
//! — any subject with *any* `ex:inZone` triple satisfies it, regardless of
//! the object. This only bites when more than one subject in the dataset
//! shares that predicate with *different* object values (e.g. six vessels
//! each with their own `inZone`) — with a single relevant subject, or a rule
//! using only variables (no constant terms at all, e.g. a `?z`/`?r`
//! transitive-closure premise), it's unobservable and everything behaves
//! correctly. The rule engine also doesn't accept string/numeric literals as
//! rule terms at all (only IRIs and variables — `"silent"` fails to parse;
//! `ex:Silent` works) and doesn't support `#` comments in rule files.
//! **Workaround:** give each fact its own dedicated predicate instead of
//! sharing one predicate with varying objects — `ex:inMarineProtectedArea
//! ex:True` / `ex:inOpenSea ex:True` instead of `ex:inZone
//! ex:MarineProtectedArea` / `ex:inZone ex:OpenSea` — so a premise's object
//! is always the one constant that predicate ever takes, which sidesteps the
//! bug entirely regardless of how many other subjects/predicates exist. See
//! `examples/echo-pipeline/ais-rules.n3` in the kolibrie-processor examples
//! for a worked example of this pattern.
//!
//! ## Per-subject capped retention (`capped_reader`)
//!
//! Kolibrie's RSP-QL windows only evict by time range — there is no API to
//! forcibly evict a specific already-admitted item early, so "keep only the
//! last N messages per subject" cannot be expressed as an RSP-QL window.
//! When `capped_reader`/`max_per_subject` are set, this processor instead
//! maintains the last `max_per_subject` messages per RDF subject itself and
//! re-runs the same Datalog rules over exactly that retained set whenever it
//! changes. **This path reasons independently of the RSP-QL engine above —
//! its output is not joined with the windowed/static streams.**
//!
//! `max_per_subject` alone only bounds each *existing* subject's own queue —
//! a stream with ever-growing subject cardinality (new sensor IDs that never
//! repeat, say) would otherwise grow the subject-tracking map without bound.
//! Set `subject_timeout` (seconds) to evict a subject's entry entirely once
//! it's gone quiet for that long. Optionally pair it with `silence_event` — an
//! N-Triples line template with a `{subject}` placeholder — to inject a fact
//! in place of the evicted subject's stale readings so your own rules can
//! react to "this subject stopped sending", e.g. to raise an alert:
//!
//! ```text
//! "silence_event": "{subject} <http://example.org/status> \"silent\" ."
//! ```
//!
//! By default every capped_reader message triggers an immediate reasoning
//! pass. Set `capped_debounce_ms` to coalesce bursts instead — wait that many
//! ms after the last change before re-running — trading latency for less
//! redundant work; omit it for the fastest per-message response.
//!
//! Internally, reasoning passes are incremental (a persistent `Reasoner` is
//! only ever fed newly-admitted triples) *except* immediately after an
//! eviction (from either the per-subject cap or `subject_timeout`), which
//! forces one full rebuild from the current retained set — Kolibrie's
//! semi-naive materialisation has no retraction/truth-maintenance support, so
//! that's the only way to guarantee correctness once a ground fact is
//! removed.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rdfc_processor_sdk::{
    bytes::Bytes,
    futures::{
        future::{select, Either},
        stream::select_all,
        Stream, StreamExt,
    },
    Processor, Reader, Slot, Writer,
};

use datalog::parser_n3_logic::parse_n3_document;
use datalog::reasoning::Reasoner;
use kolibrie::rsp_engine::{OperationMode, RSPBuilder, ResultConsumer, SimpleR2R};
use shared::dataset_index::DatasetIndex;
use shared::dictionary::Dictionary;
use shared::query::SyncPolicy;
use shared::rule::Rule;
use shared::rule_index::RuleIndex;
use shared::terms::Term;
use shared::triple::Triple;

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct Args {
    readers: Vec<Reader>,
    writer: Writer,
    #[serde(default)]
    static_reader: Option<Reader>,
    #[serde(default)]
    capped_reader: Option<Reader>,
    #[serde(default)]
    max_per_subject: u64,
    /// Seconds of inactivity after which a subject's capped_reader entry is
    /// evicted (bounds `per_subject` memory even for ever-growing subject
    /// cardinality). Requires `capped_reader`.
    #[serde(default)]
    subject_timeout: Option<u64>,
    /// N-Triples-line template (with a `{subject}` placeholder) inserted in
    /// place of an evicted subject's retained readings when `subject_timeout`
    /// fires, so rules can react to "this subject went silent". Requires
    /// `subject_timeout`.
    #[serde(default)]
    silence_event: Option<String>,
    /// Coalesce capped_reader reasoning passes: wait this many ms after the
    /// last change before re-running, instead of on every message. Requires
    /// `capped_reader`; omit for the fastest (immediate) response.
    #[serde(default)]
    capped_debounce_ms: Option<u64>,
    #[serde(default)]
    rsp_query: Option<String>,
    #[serde(default)]
    rsp_query_file: Option<String>,
    #[serde(default)]
    rules: Option<String>,
    #[serde(default)]
    rules_file: Option<String>,
    #[serde(default)]
    sync_policy: Option<String>,
}

// ── Processor ─────────────────────────────────────────────────────────────────

pub struct Kolibrie {
    readers: Slot<Vec<Reader>>,
    writer: Slot<Writer>,
    static_reader: Slot<Option<Reader>>,
    capped_reader: Slot<Option<Reader>>,
    max_per_subject: u64,
    subject_timeout: Option<u64>,
    silence_event: Option<String>,
    capped_debounce_ms: Option<u64>,
    rsp_query: Option<String>,
    rsp_query_file: Option<String>,
    rules: Option<String>,
    rules_file: Option<String>,
    sync_policy: Option<String>,
}

impl Processor for Kolibrie {
    type Args = Args;

    fn new(args: Args) -> Self {
        Kolibrie {
            readers: Slot::new(args.readers),
            writer: Slot::new(args.writer),
            static_reader: Slot::new(args.static_reader),
            capped_reader: Slot::new(args.capped_reader),
            max_per_subject: args.max_per_subject,
            subject_timeout: args.subject_timeout,
            silence_event: args.silence_event,
            capped_debounce_ms: args.capped_debounce_ms,
            rsp_query: args.rsp_query,
            rsp_query_file: args.rsp_query_file,
            rules: args.rules,
            rules_file: args.rules_file,
            sync_policy: args.sync_policy,
        }
    }

    async fn transform(&self) -> Result<(), String> {
        let readers_raw = self.readers.take();
        let writer = self.writer.take();
        let static_reader = self.static_reader.take();
        let capped_reader = self.capped_reader.take();

        if capped_reader.is_some() && self.max_per_subject == 0 {
            return Err(
                "kolibrie processor: 'max_per_subject' must be > 0 when 'capped_reader' is set"
                    .to_string(),
            );
        }
        if capped_reader.is_none()
            && (self.subject_timeout.is_some()
                || self.silence_event.is_some()
                || self.capped_debounce_ms.is_some())
        {
            return Err(
                "kolibrie processor: 'subject_timeout', 'silence_event' and 'capped_debounce_ms' require 'capped_reader' to be set"
                    .to_string(),
            );
        }
        if self.silence_event.is_some() && self.subject_timeout.is_none() {
            return Err(
                "kolibrie processor: 'silence_event' requires 'subject_timeout' to be set"
                    .to_string(),
            );
        }

        let rsp_query_text = load_text(
            self.rsp_query.clone(),
            self.rsp_query_file.clone(),
            "rsp_query",
            "rsp_query_file",
        )?
        .ok_or_else(|| {
            "kolibrie processor: either 'rsp_query' or 'rsp_query_file' must be provided"
                .to_string()
        })?;

        let rules_text = load_text(
            self.rules.clone(),
            self.rules_file.clone(),
            "rules",
            "rules_file",
        )?;

        let sync_policy = parse_sync_policy(self.sync_policy.as_deref())?;

        // Parse the (optional) N3 Datalog rules once, against one shared dictionary.
        // `parsed_rules` + `rules_dict` are reused verbatim by the `capped_reader`
        // path below; for the RSP-QL engine, each rule is re-serialised back into
        // prefix-free N3 text (see `rule_to_n3`) — `SimpleR2R::load_rules` parses
        // rule text one rule at a time and only recognises `@prefix` lines
        // immediately preceding the rule currently being parsed, so a single
        // shared `@prefix` block (the convention documented above and used by
        // this crate's own tests) would silently stop applying after the first
        // rule if we handed the raw text straight through.
        let mut seed_reasoner = Reasoner::new();
        let parsed_rules: Vec<Rule> = match &rules_text {
            Some(text) => parse_rules(text, &mut seed_reasoner)?,
            None => Vec::new(),
        };
        let rules_dict: Arc<RwLock<Dictionary>> = Arc::clone(&seed_reasoner.dictionary);
        let reconstructed_rules: Option<String> = if parsed_rules.is_empty() {
            None
        } else {
            let dict = rules_dict.read().unwrap();
            Some(
                parsed_rules
                    .iter()
                    .map(|r| rule_to_n3(r, &dict))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
        };

        eprintln!(
            "[kolibrie] loaded {} rule(s), {} reader(s){}{}",
            parsed_rules.len(),
            readers_raw.len(),
            if static_reader.is_some() { ", 1 static reader" } else { "" },
            if capped_reader.is_some() { ", 1 capped reader" } else { "" },
        );

        // ── Build the RSP-QL engine ───────────────────────────────────────────
        let output_buf: Arc<Mutex<Vec<Vec<(String, String)>>>> = Arc::new(Mutex::new(Vec::new()));
        let output_buf_cb = Arc::clone(&output_buf);
        let consumer = ResultConsumer {
            function: Arc::new(move |row: Vec<(String, String)>| {
                output_buf_cb.lock().unwrap().push(row);
            }),
        };

        let mut builder = RSPBuilder::<Triple, Vec<(String, String)>>::new()
            .add_rsp_ql_query(&rsp_query_text)
            .add_r2r(Box::new(SimpleR2R::new()))
            .add_consumer(consumer)
            .set_operation_mode(OperationMode::SingleThread)
            .set_sync_policy(sync_policy);
        if let Some(rules) = &reconstructed_rules {
            builder = builder.add_rules(rules);
        }
        let mut engine = builder.build()?;

        eprintln!(
            "[kolibrie] RSP-QL engine ready ({} window(s))",
            engine.get_window_info().len()
        );

        // ── Merge every configured reader into one polling loop ──────────────
        enum Incoming {
            Window(usize, Bytes),
            Static(Bytes),
            Capped(Bytes),
        }

        let mut stream_iris: Vec<String> = Vec::with_capacity(readers_raw.len());
        let mut merged_streams: Vec<Pin<Box<dyn Stream<Item = Incoming> + Send>>> = Vec::new();
        for (idx, reader) in readers_raw.into_iter().enumerate() {
            stream_iris.push(reader.channel_uri().to_string());
            merged_streams.push(Box::pin(
                reader.into_stream().map(move |b| Incoming::Window(idx, b)),
            ));
        }
        if let Some(reader) = static_reader {
            merged_streams.push(Box::pin(reader.into_stream().map(Incoming::Static)));
        }
        if let Some(reader) = capped_reader {
            merged_streams.push(Box::pin(reader.into_stream().map(Incoming::Capped)));
        }
        let mut merged = select_all(merged_streams);

        // Dedup for RSP-QL output (re-materialised-but-unchanged facts aren't
        // re-emitted every firing) and state for the independent capped-retention
        // path.
        let mut written: HashSet<String> = HashSet::new();
        let mut per_subject: HashMap<u32, VecDeque<Triple>> = HashMap::new();
        let mut capped_written: HashSet<Triple> = HashSet::new();
        let mut last_seen: HashMap<u32, Instant> = HashMap::new();
        // Triples admitted since the last capped-reasoning pass but not yet
        // inserted into `capped_reasoner` — only used on the fast (no-rebuild) path.
        let mut pending_inserts: Vec<Triple> = Vec::new();
        let mut capped_needs_rebuild = false;
        let mut capped_pending = false;
        let mut capped_last_change = Instant::now();

        // Built once (rules never change during the stream) and then mutated
        // in place: only its `dataset_index` gets reset, and only when an
        // eviction actually happened — see `run_capped_pass`.
        let mut capped_reasoner: Option<Reasoner> = if self.max_per_subject > 0 {
            Some(build_capped_reasoner(&parsed_rules, &rules_dict))
        } else {
            None
        };

        // A ticker thread (plain std::thread + std::thread::sleep — no ambient
        // Tokio runtime needed, see rdfc_processor_sdk's module docs) drives
        // `subject_timeout` sweeps and `capped_debounce_ms` flushes. It is kept
        // OUT of `merged` deliberately: `select_all` only ends once every one of
        // its streams ends, and this ticker never ends on its own — merging it in
        // would make `transform()` hang forever even after every real reader
        // closes. Instead it's polled via `futures::future::select` below, so the
        // loop can end as soon as `merged` is exhausted, regardless of the ticker.
        let mut tick_rx: Option<tokio::sync::mpsc::UnboundedReceiver<()>> =
            if self.subject_timeout.is_some() || self.capped_debounce_ms.is_some() {
                let tick_ms = self.capped_debounce_ms.unwrap_or(250).min(250).max(20);
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
                std::thread::spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(tick_ms));
                    if tx.send(()).is_err() {
                        break; // main loop ended; stop ticking
                    }
                });
                Some(rx)
            } else {
                None
            };

        loop {
            let item = if let Some(rx) = tick_rx.as_mut() {
                match select(Box::pin(merged.next()), Box::pin(rx.recv())).await
                {
                    Either::Left((Some(item), _)) => item,
                    Either::Left((None, _)) => break, // every real reader closed
                    Either::Right((_tick, _)) => {
                        handle_capped_tick(
                            self.subject_timeout,
                            self.silence_event.as_deref(),
                            self.capped_debounce_ms,
                            &mut per_subject,
                            &mut last_seen,
                            &rules_dict,
                            &mut capped_needs_rebuild,
                            &mut capped_pending,
                            &mut capped_last_change,
                        );
                        if capped_pending
                            && ready_to_flush(self.capped_debounce_ms, capped_last_change)
                        {
                            if let Some(reasoner) = capped_reasoner.as_mut() {
                                run_capped_pass(
                                    reasoner,
                                    &per_subject,
                                    &mut pending_inserts,
                                    &mut capped_needs_rebuild,
                                    &mut capped_written,
                                    &rules_dict,
                                    &writer,
                                )
                                .await?;
                            }
                            capped_pending = false;
                        }
                        continue;
                    }
                }
            } else {
                match merged.next().await {
                    Some(item) => item,
                    None => break,
                }
            };

            match item {
                Incoming::Window(idx, bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    if let Some(nt) = rdf_to_ntriples(&text) {
                        let triples = engine.parse_data(&nt);
                        let ts = now_secs();
                        for triple in triples {
                            engine.add_to_stream(&stream_iris[idx], triple, ts);
                        }
                    }
                }
                Incoming::Static(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    if let Some(nt) = rdf_to_ntriples(&text) {
                        engine.add_static_ntriples(&nt);
                    }
                }
                Incoming::Capped(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    let batch = parse_rdf(&text, &mut *rules_dict.write().unwrap());
                    if !batch.is_empty() {
                        let now = Instant::now();
                        for triple in batch {
                            last_seen.insert(triple.subject, now);
                            let evicted =
                                admit_capped(&mut per_subject, triple.clone(), self.max_per_subject);
                            if evicted {
                                capped_needs_rebuild = true;
                            }
                            pending_inserts.push(triple);
                        }
                        capped_pending = true;
                        capped_last_change = now;
                        if ready_to_flush(self.capped_debounce_ms, capped_last_change) {
                            if let Some(reasoner) = capped_reasoner.as_mut() {
                                run_capped_pass(
                                    reasoner,
                                    &per_subject,
                                    &mut pending_inserts,
                                    &mut capped_needs_rebuild,
                                    &mut capped_written,
                                    &rules_dict,
                                    &writer,
                                )
                                .await?;
                            }
                            capped_pending = false;
                        }
                    }
                }
            }

            drain_rsp_output(&output_buf, &mut written, &writer).await?;
        }

        engine.stop();
        drain_rsp_output(&output_buf, &mut written, &writer).await?;
        // Flush any capped-reader change still pending behind an unexpired debounce.
        if capped_pending {
            if let Some(reasoner) = capped_reasoner.as_mut() {
                run_capped_pass(
                    reasoner,
                    &per_subject,
                    &mut pending_inserts,
                    &mut capped_needs_rebuild,
                    &mut capped_written,
                    &rules_dict,
                    &writer,
                )
                .await?;
            }
        }

        writer.close();
        Ok(())
    }
}

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#KolibrieProcessor", Kolibrie);
rdfc_processor_sdk::rdfc_export_processors!();

// ── RSP-QL engine helpers ─────────────────────────────────────────────────────

/// Drain whatever the RSP engine's `ResultConsumer` produced synchronously during
/// the last `add_to_stream`/`add_static_ntriples`/`stop` call, converting rows
/// that bind `?s ?p ?o` into deduplicated N-Triples output.
async fn drain_rsp_output(
    output_buf: &Arc<Mutex<Vec<Vec<(String, String)>>>>,
    written: &mut HashSet<String>,
    writer: &Writer,
) -> Result<(), String> {
    let rows: Vec<Vec<(String, String)>> = std::mem::take(&mut *output_buf.lock().unwrap());
    if rows.is_empty() {
        return Ok(());
    }
    let mut nt = String::new();
    for row in &rows {
        if let Some(line) = binding_to_ntriple(row) {
            if written.insert(line.clone()) {
                nt.push_str(&line);
            }
        }
    }
    if !nt.is_empty() {
        writer.write_str(&nt).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Reconstruct one N-Triples line from a WHERE-clause row that bound `?s ?p ?o`.
/// Returns `None` for rows missing any of the three (e.g. a query written
/// without that convention), so callers can skip it rather than emit garbage.
fn binding_to_ntriple(row: &[(String, String)]) -> Option<String> {
    let get = |key: &str| row.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
    let (s, p, o) = (get("s")?, get("p")?, get("o")?);
    Some(format!("{} {} {} .\n", nt_term(s), nt_term(p), nt_term(o)))
}

fn now_secs() -> usize {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
}

/// Resolve the engine-level default `SyncPolicy` from the `sync_policy` arg.
fn parse_sync_policy(value: Option<&str>) -> Result<SyncPolicy, String> {
    match value.unwrap_or("wait").trim().to_ascii_lowercase().as_str() {
        "wait" => Ok(SyncPolicy::Wait),
        "steal" => Ok(SyncPolicy::Steal),
        other => Err(format!(
            "kolibrie processor: unknown sync_policy '{other}' (expected 'wait' or 'steal')"
        )),
    }
}

/// Re-serialise a parsed [`Rule`] back into `{ ... } => { ... }` N3 text using
/// fully-qualified `<...>` IRIs (decoded from `dict`) instead of prefixed names,
/// so the reconstructed text never needs prefix resolution — see the note on
/// `SimpleR2R::load_rules` above.
fn rule_to_n3(rule: &Rule, dict: &Dictionary) -> String {
    let premise: Vec<String> = rule
        .premise
        .iter()
        .map(|(s, p, o)| pattern_to_n3(s, p, o, dict))
        .collect();
    let conclusion: Vec<String> = rule
        .conclusion
        .iter()
        .map(|(s, p, o)| pattern_to_n3(s, p, o, dict))
        .collect();
    format!("{{ {} }} => {{ {} }} .", premise.join(" "), conclusion.join(" "))
}

fn pattern_to_n3(s: &Term, p: &Term, o: &Term, dict: &Dictionary) -> String {
    format!(
        "{} {} {} .",
        term_to_n3(s, dict),
        term_to_n3(p, dict),
        term_to_n3(o, dict)
    )
}

fn term_to_n3(term: &Term, dict: &Dictionary) -> String {
    match term {
        Term::Variable(name) => format!("?{name}"),
        Term::Constant(id) => dict.decode(*id).map(nt_term).unwrap_or_default(),
        // RDF-star quoted-triple terms aren't used by this processor's rule format.
        Term::QuotedTriple(_) => String::new(),
    }
}

/// Resolve either an inline value or a file path into text. Returns `Ok(None)`
/// if neither is given (both optional), errors if both are given the wrong way.
fn load_text(
    inline: Option<String>,
    file: Option<String>,
    inline_name: &str,
    file_name: &str,
) -> Result<Option<String>, String> {
    match (inline, file) {
        (Some(text), _) => Ok(Some(text)),
        (None, Some(path)) => std::fs::read_to_string(&path)
            .map(Some)
            .map_err(|e| format!("cannot read {file_name} file '{path}': {e}")),
        (None, None) => {
            let _ = inline_name; // kept for symmetry with error messages at call sites
            Ok(None)
        }
    }
}

/// Parse N3 rules from text using Kolibrie's built-in parser.
/// The `seed_reasoner`'s dictionary is populated as a side effect (IRIs are
/// encoded into it).
fn parse_rules(text: &str, seed_reasoner: &mut Reasoner) -> Result<Vec<Rule>, String> {
    match parse_n3_document(text, seed_reasoner) {
        Ok((_, (_, rules))) => Ok(rules),
        Err(e) => Err(format!("failed to parse N3 rules: {e:?}")),
    }
}

// ── Capped-per-subject retention (Stage 2, independent of the RSP-QL engine) ──

/// Push `triple` onto its subject's retained queue, evicting the oldest entry
/// once the queue exceeds `max_per_subject`. Returns whether an eviction
/// happened, so the caller can tell `run_capped_pass` when it must fall back to
/// a full rebuild instead of a cheap incremental insert (see that function's
/// doc comment for why). A standalone function so eviction itself can be
/// unit-tested directly, decoupled from `run_capped_pass`'s append-only
/// (no-retraction) output stream — a fact already emitted stays emitted even
/// after its source triple is evicted here, same as this crate's previous
/// time-windowed `run_windowed` always behaved.
fn admit_capped(
    per_subject: &mut HashMap<u32, VecDeque<Triple>>,
    triple: Triple,
    max_per_subject: u64,
) -> bool {
    let deque = per_subject.entry(triple.subject).or_default();
    deque.push_back(triple);
    let mut evicted = false;
    while deque.len() as u64 > max_per_subject {
        deque.pop_front();
        evicted = true;
    }
    evicted
}

/// Find subjects in `last_seen` that haven't been touched within `timeout` of
/// `now`, removing them from both `last_seen` and `per_subject` (this is what
/// actually bounds `per_subject`'s memory for ever-growing subject
/// cardinality — `admit_capped` alone only bounds each *existing* subject's
/// queue length). Returns the evicted subject ids so the caller can decide
/// whether to inject a `silence_event`. Takes `now` as a parameter (rather
/// than calling `Instant::now()` itself) so it can be unit-tested with
/// synthetic timestamps instead of real sleeps.
fn sweep_stale_subjects(
    per_subject: &mut HashMap<u32, VecDeque<Triple>>,
    last_seen: &mut HashMap<u32, Instant>,
    now: Instant,
    timeout: Duration,
) -> Vec<u32> {
    let stale: Vec<u32> = last_seen
        .iter()
        .filter(|(_, &t)| now.duration_since(t) >= timeout)
        .map(|(&s, _)| s)
        .collect();
    for subject in &stale {
        last_seen.remove(subject);
        per_subject.remove(subject);
    }
    stale
}

/// Build the N-Triples line(s) for a `silence_event` template, substituting
/// every `{subject}` occurrence with `subject`'s decoded, correctly-formatted
/// term (`<iri>` or `_:bnode`), then parsing the result against `dict`.
/// Returns an empty `Vec` (with a warning logged) if `subject` can't be
/// decoded or the substituted template doesn't parse as valid RDF.
fn build_silence_triples(template: &str, subject: u32, dict: &mut Dictionary) -> Vec<Triple> {
    let Some(subject_str) = dict.decode(subject).map(|s| s.to_string()) else {
        eprintln!("[kolibrie] warning: silence_event: could not decode evicted subject");
        return Vec::new();
    };
    let text = template.replace("{subject}", &nt_term(&subject_str));
    let triples = parse_rdf(&text, dict);
    if triples.is_empty() {
        eprintln!(
            "[kolibrie] warning: silence_event template produced no valid triples (subject: {subject_str})"
        );
    }
    triples
}

/// Build the persistent capped-retention [`Reasoner`], loaded with `rules`
/// once. Only ever called once per stream — `run_capped_pass` mutates the
/// same instance's `dataset_index` in place afterward.
fn build_capped_reasoner(rules: &[Rule], dict: &Arc<RwLock<Dictionary>>) -> Reasoner {
    let mut reasoner = Reasoner {
        dictionary: Arc::clone(dict),
        rules: Vec::new(),
        dataset_index: DatasetIndex::new(),
        rule_index: RuleIndex::new(),
        constraints: Vec::new(),
        probability_seeds: std::collections::HashMap::new(),
    };
    for rule in rules {
        if let Err(e) = reasoner.try_add_rule(rule.clone()) {
            eprintln!("[kolibrie] warning: skipping rule: {e}");
        }
    }
    reasoner
}

/// Whether a pending capped-reader change should be flushed now: immediately
/// when no debounce is configured, otherwise once `capped_debounce_ms` has
/// elapsed since the last change.
fn ready_to_flush(debounce_ms: Option<u64>, last_change: Instant) -> bool {
    match debounce_ms {
        Some(ms) => Instant::now().duration_since(last_change) >= Duration::from_millis(ms),
        None => true,
    }
}

/// Handle one ticker firing: sweep `subject_timeout`-expired subjects (if
/// configured), optionally injecting `silence_event` triples in place of each
/// evicted subject's stale readings and marking a pass as pending. Does not
/// itself run the reasoning pass — the caller checks `ready_to_flush`.
#[allow(clippy::too_many_arguments)]
fn handle_capped_tick(
    subject_timeout: Option<u64>,
    silence_event: Option<&str>,
    _capped_debounce_ms: Option<u64>,
    per_subject: &mut HashMap<u32, VecDeque<Triple>>,
    last_seen: &mut HashMap<u32, Instant>,
    dict: &Arc<RwLock<Dictionary>>,
    needs_rebuild: &mut bool,
    pending: &mut bool,
    last_change: &mut Instant,
) {
    let Some(timeout_secs) = subject_timeout else {
        return;
    };
    let now = Instant::now();
    let stale = sweep_stale_subjects(per_subject, last_seen, now, Duration::from_secs(timeout_secs));
    if stale.is_empty() {
        return;
    }

    if let Some(template) = silence_event {
        let mut dict_w = dict.write().unwrap();
        for subject in &stale {
            let triples = build_silence_triples(template, *subject, &mut dict_w);
            if !triples.is_empty() {
                // Replace (not append to) the evicted subject's retained set —
                // its stale readings are gone; only the fact that it went
                // silent remains.
                per_subject.insert(*subject, VecDeque::from(triples));
            }
        }
    }

    *needs_rebuild = true;
    *pending = true;
    *last_change = now;
}

/// Re-run capped-retention reasoning: a cheap incremental insert of whatever's
/// queued in `pending_inserts` when nothing was evicted since the last pass,
/// or — only when `needs_rebuild` is set (an eviction happened, whether from
/// `admit_capped`'s per-subject cap or a `subject_timeout` sweep) — a full
/// rebuild of `reasoner`'s ground facts from `per_subject`.
///
/// A full rebuild is genuinely required after an eviction: `Reasoner`'s
/// semi-naive materialisation (see `datalog::reasoning::materialisation::
/// semi_naive`) has no retraction/truth-maintenance support — deleting one
/// ground fact from a live `DatasetIndex` (which `DatasetIndex::delete_triple`
/// does support) would NOT retract facts previously derived *from* it, so a
/// persisted reasoner could keep deriving new conclusions from stale premises
/// forever. Rebuilding from the current `per_subject` contents is the only way
/// this engine can guarantee correctness after eviction. When nothing was
/// evicted, though, plain incremental insertion is exactly how this crate's
/// non-evicting `run_incremental` mode has always worked, and is safe here for
/// the same reason.
#[allow(clippy::too_many_arguments)]
async fn run_capped_pass(
    reasoner: &mut Reasoner,
    per_subject: &HashMap<u32, VecDeque<Triple>>,
    pending_inserts: &mut Vec<Triple>,
    needs_rebuild: &mut bool,
    written: &mut HashSet<Triple>,
    dict: &Arc<RwLock<Dictionary>>,
    writer: &Writer,
) -> Result<(), String> {
    if *needs_rebuild {
        reasoner.dataset_index = DatasetIndex::new();
        for deque in per_subject.values() {
            for triple in deque {
                reasoner.insert_ground_triple(triple.clone());
            }
        }
        *needs_rebuild = false;
        pending_inserts.clear();
    } else {
        for triple in pending_inserts.drain(..) {
            reasoner.insert_ground_triple(triple);
        }
    }

    let derived = reasoner.infer_new_facts_semi_naive();
    let mut new_facts: Vec<Triple> = Vec::new();
    for fact in derived {
        if written.insert(fact.clone()) {
            new_facts.push(fact);
        }
    }

    if !new_facts.is_empty() {
        let nt = triples_to_ntriples(&new_facts, &dict.read().unwrap());
        writer.write_str(&nt).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

// ── RDF parsing ───────────────────────────────────────────────────────────────
//
// Two flavours are needed:
// - `rdf_to_ntriples` (dictionary-free): used for data fed to the RSP-QL engine,
//   which does its own dictionary-encoding internally (`engine.parse_data`,
//   `engine.add_static_ntriples`) so every term stays consistent with the
//   engine's compiled query plan.
// - `parse_rdf` (dictionary-based, unchanged from before): used only by the
//   independent `capped_reader` path, which runs its own `Reasoner` sharing
//   `rules_dict`.

/// Parse an RDF document of unknown (but auto-detected) syntax straight into an
/// N-Triples string, without touching any dictionary.
fn rdf_to_ntriples(text: &str) -> Option<String> {
    if let Some(nt) = trig_to_ntriples(text) {
        return Some(nt);
    }
    if let Some(nt) = nquads_to_ntriples(text) {
        return Some(nt);
    }
    eprintln!("[kolibrie] warning: could not parse message as Turtle/TriG/N-Triples/N-Quads");
    None
}

fn trig_to_ntriples(text: &str) -> Option<String> {
    let mut out = String::new();
    for quad in oxttl::TriGParser::new().for_slice(text.as_bytes()) {
        out.push_str(&quad_to_ntriple_line(quad.ok()?));
    }
    Some(out)
}

fn nquads_to_ntriples(text: &str) -> Option<String> {
    let mut out = String::new();
    for quad in oxttl::NQuadsParser::new().for_slice(text.as_bytes()) {
        out.push_str(&quad_to_ntriple_line(quad.ok()?));
    }
    Some(out)
}

fn quad_to_ntriple_line(quad: oxrdf::Quad) -> String {
    format!(
        "{} {} {} .\n",
        nt_term(&subject_to_string(&quad.subject)),
        nt_term(quad.predicate.as_str()),
        nt_term(&term_to_string(&quad.object)),
    )
}

/// Parse an RDF document of unknown (but auto-detected) syntax into encoded
/// [`Triple`]s using `dict`. Only used by the `capped_reader` path.
fn parse_rdf(text: &str, dict: &mut Dictionary) -> Vec<Triple> {
    if let Some(triples) = parse_trig(text, dict) {
        return triples;
    }
    if let Some(triples) = parse_nquads(text, dict) {
        return triples;
    }
    eprintln!("[kolibrie] warning: could not parse message as Turtle/TriG/N-Triples/N-Quads");
    Vec::new()
}

fn parse_trig(text: &str, dict: &mut Dictionary) -> Option<Vec<Triple>> {
    let mut triples = Vec::new();
    for quad in oxttl::TriGParser::new().for_slice(text.as_bytes()) {
        triples.push(encode_quad(quad.ok()?, dict));
    }
    Some(triples)
}

fn parse_nquads(text: &str, dict: &mut Dictionary) -> Option<Vec<Triple>> {
    let mut triples = Vec::new();
    for quad in oxttl::NQuadsParser::new().for_slice(text.as_bytes()) {
        triples.push(encode_quad(quad.ok()?, dict));
    }
    Some(triples)
}

/// Encode an `oxrdf::Quad` (graph name is dropped) into a dictionary-backed [`Triple`].
fn encode_quad(quad: oxrdf::Quad, dict: &mut Dictionary) -> Triple {
    let s = subject_to_string(&quad.subject);
    let p = quad.predicate.as_str().to_owned();
    let o = term_to_string(&quad.object);
    Triple {
        subject: dict.encode(&s),
        predicate: dict.encode(&p),
        object: dict.encode(&o),
    }
}

/// Format an `oxrdf::NamedOrBlankNode` to match Kolibrie's dictionary convention.
fn subject_to_string(subject: &oxrdf::NamedOrBlankNode) -> String {
    match subject {
        oxrdf::NamedOrBlankNode::NamedNode(n) => n.as_str().to_owned(),
        oxrdf::NamedOrBlankNode::BlankNode(b) => b.to_string(),
    }
}

/// Format an `oxrdf::Term` to match Kolibrie's dictionary convention.
///
/// IRIs are stored without angle brackets; `Literal`'s `Display` impl already
/// produces `"value"`, `"value"@lang` or `"value"^^<datatype>`, matching the
/// convention exactly.
fn term_to_string(term: &oxrdf::Term) -> String {
    match term {
        oxrdf::Term::NamedNode(n) => n.as_str().to_owned(),
        oxrdf::Term::BlankNode(b) => b.to_string(),
        oxrdf::Term::Literal(l) => l.to_string(),
        #[allow(unreachable_patterns)]
        _ => term.to_string(), // e.g. RDF-star quoted triples, not used here
    }
}

// ── N-Triples serialiser ──────────────────────────────────────────────────────

/// Encode a slice of [`Triple`]s as an N-Triples string.
fn triples_to_ntriples(triples: &[Triple], dict: &Dictionary) -> String {
    let mut out = String::new();
    for triple in triples {
        let Some(s) = dict.decode(triple.subject) else { continue };
        let Some(p) = dict.decode(triple.predicate) else { continue };
        let Some(o) = dict.decode(triple.object) else { continue };
        out.push_str(&format!(
            "{} {} {} .\n",
            nt_term(s),
            nt_term(p),
            nt_term(o)
        ));
    }
    out
}

/// Format a dictionary term as an N-Triples token.
///
/// - Blank nodes (`_:…`) and literals (`"…`) are emitted as-is.
/// - Everything else is treated as an IRI and wrapped in `<…>`.
fn nt_term(term: &str) -> String {
    if term.starts_with("_:") || term.starts_with('"') {
        term.to_owned()
    } else {
        format!("<{}>", term)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rdfc_processor_sdk::make_writer;
    use rdfc_processor_sdk::testing::*;
    use serde_json::json;

    // Simple ancestry rules in N3 format.
    const RULES: &str = r#"
@prefix ex: <http://example.org/> .

{ ?x ex:parent ?y } => { ?x ex:ancestor ?y }
{ ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
"#;

    const SINGLE_STREAM_QUERY: &str = r#"REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :win ON <mock://reader> [RANGE 3600 STEP 1]
WHERE {
    WINDOW :win { ?s ?p ?o }
}"#;

    // Two N-Triples messages:
    //   message 1: Alice parent Bob
    //   message 2: Bob   parent Charlie
    const MSG1: &[u8] =
        b"<http://example.org/Alice> <http://example.org/parent> <http://example.org/Bob> .";
    const MSG2: &[u8] =
        b"<http://example.org/Bob> <http://example.org/parent> <http://example.org/Charlie> .";

    async fn run_processor(messages: Vec<Vec<u8>>) -> Vec<u8> {
        let reader = make_test_reader("mock://reader", messages);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [{"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"}],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": SINGLE_STREAM_QUERY,
                "rules": RULES
            }),
            [("reader", reader)],
            [("writer", writer)],
        );

        let (result, (received, _)) =
            tokio::join!(proc.transform(), collect_writer(bridge));
        result.unwrap();
        received.concat()
    }

    #[tokio::test]
    async fn derives_ancestor_from_single_message() {
        let output = run_processor(vec![MSG1.to_vec()]).await;
        let text = String::from_utf8(output).unwrap();
        assert!(
            text.contains("ancestor"),
            "expected 'ancestor' in output, got: {text}"
        );
        assert!(
            text.contains("Alice"),
            "expected 'Alice' in output, got: {text}"
        );
    }

    #[tokio::test]
    async fn derives_transitive_ancestor_across_messages() {
        let output = run_processor(vec![MSG1.to_vec(), MSG2.to_vec()]).await;
        let text = String::from_utf8(output).unwrap();
        assert!(
            text.contains("ancestor"),
            "expected 'ancestor' in output, got: {text}"
        );
    }

    #[tokio::test]
    async fn empty_stream_closes_writer_cleanly() {
        let reader = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [{"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"}],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": SINGLE_STREAM_QUERY,
                "rules": RULES
            }),
            [("reader", reader)],
            [("writer", writer)],
        );

        let (result, (received, closed)) =
            tokio::join!(proc.transform(), collect_writer(bridge));
        result.unwrap();
        assert!(received.is_empty());
        assert!(closed);
    }

    #[tokio::test]
    async fn two_streams_at_different_velocities_join_via_static_pattern() {
        // High-velocity stream: sensor readings. Medium-velocity stream: sensor
        // metadata. Joined via the shared `?sensor` variable, each window with
        // its own RANGE.
        const HIGH_MSG: &[u8] =
            b"<http://example.org/r1> <http://example.org/hasSensor> <http://example.org/s1> .";
        const MED_MSG: &[u8] =
            b"<http://example.org/s1> <http://example.org/status> <http://example.org/ok> .";

        let query = r#"REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :highWin ON <mock://high> [RANGE 3600 STEP 1]
FROM NAMED WINDOW :medWin ON <mock://medium> [RANGE 3600 STEP 1]
WHERE {
    WINDOW :highWin { ?s ?p ?sensor }
    WINDOW :medWin { ?sensor ?p2 ?o }
}"#;

        let high = make_test_reader("mock://high", vec![HIGH_MSG.to_vec()]);
        let medium = make_test_reader("mock://medium", vec![MED_MSG.to_vec()]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [
                    {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://high"},
                    {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://medium"}
                ],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": query
            }),
            [("high", high), ("medium", medium)],
            [("writer", writer)],
        );

        let (result, (received, _)) = tokio::join!(proc.transform(), collect_writer(bridge));
        result.unwrap();
        let text = String::from_utf8(received.concat()).unwrap();
        assert!(
            text.contains("hasSensor"),
            "expected the high-velocity triple in output, got: {text}"
        );
    }

    // Eviction correctness itself is verified precisely (white-box) by
    // `admit_capped_evicts_oldest_beyond_cap` below. This integration test only
    // checks the `capped_reader` path is wired up end-to-end: rules apply and
    // output reaches the writer. It deliberately does NOT assert that reading
    // "1" is absent from the output — this processor's output has always been
    // an append-only stream of newly-derived facts (see `run_capped_reasoner`
    // and the original `run_windowed` it mirrors): once a fact is emitted it
    // stays emitted, even after its source triple is later evicted from the
    // retained set. Eviction changes what's fed to *future* reasoning passes,
    // not what was already written.
    #[tokio::test]
    async fn capped_reader_applies_rules_and_reaches_writer() {
        const RULES2: &str = r#"
@prefix ex: <http://example.org/> .
{ ?x ex:hasReading ?y } => { ?x ex:hasRecentReading ?y }
"#;
        let msg = |n: u32| -> Vec<u8> {
            format!(
                "<http://example.org/sensor> <http://example.org/hasReading> \"{n}\"^^<http://www.w3.org/2001/XMLSchema#integer> ."
            )
            .into_bytes()
        };

        let capped = make_test_reader("mock://capped", vec![msg(1), msg(2), msg(3)]);
        let dummy_window = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [{"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"}],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": SINGLE_STREAM_QUERY,
                "rules": RULES2,
                "capped_reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://capped"},
                "max_per_subject": 2
            }),
            [("reader", dummy_window), ("capped", capped)],
            [("writer", writer)],
        );

        let (result, (received, _)) = tokio::join!(proc.transform(), collect_writer(bridge));
        result.unwrap();
        let text = String::from_utf8(received.concat()).unwrap();
        assert!(
            text.contains("hasRecentReading"),
            "expected rule-derived facts in output, got: {text}"
        );
        assert!(text.contains('3'), "expected reading 3 in output, got: {text}");
    }

    #[test]
    fn admit_capped_evicts_oldest_beyond_cap() {
        let mut dict = Dictionary::new();
        let subject = dict.encode("http://example.org/sensor");
        let predicate = dict.encode("http://example.org/hasReading");

        let mut per_subject: HashMap<u32, VecDeque<Triple>> = HashMap::new();
        for n in 1..=3u32 {
            let object = dict.encode(&n.to_string());
            admit_capped(
                &mut per_subject,
                Triple { subject, predicate, object },
                2,
            );
        }

        let retained = &per_subject[&subject];
        assert_eq!(retained.len(), 2, "expected exactly 2 retained, got {retained:?}");
        let values: Vec<&str> = retained
            .iter()
            .map(|t| dict.decode(t.object).unwrap())
            .collect();
        assert_eq!(values, vec!["2", "3"], "expected reading 1 to have been evicted");
    }

    #[test]
    fn admit_capped_reports_no_eviction_under_cap() {
        let mut per_subject: HashMap<u32, VecDeque<Triple>> = HashMap::new();
        let t = Triple { subject: 1, predicate: 2, object: 3 };
        assert!(
            !admit_capped(&mut per_subject, t, 5),
            "should not report eviction while under the cap"
        );
    }

    #[test]
    fn sweep_stale_subjects_evicts_after_timeout_and_bounds_per_subject() {
        let mut per_subject: HashMap<u32, VecDeque<Triple>> = HashMap::new();
        let mut last_seen: HashMap<u32, Instant> = HashMap::new();

        let stale_subject = 1u32;
        let fresh_subject = 2u32;
        let t0 = Instant::now();
        per_subject.insert(stale_subject, VecDeque::from([Triple { subject: 1, predicate: 9, object: 9 }]));
        per_subject.insert(fresh_subject, VecDeque::from([Triple { subject: 2, predicate: 9, object: 9 }]));
        // Synthetic timestamps rather than real sleeps: subject 1 was last seen
        // 10s "ago" relative to `now`, subject 2 only 1s ago.
        last_seen.insert(stale_subject, t0);
        last_seen.insert(fresh_subject, t0);
        let now = t0 + Duration::from_secs(10);
        // Bump the fresh subject's last-seen to 1s before `now`.
        last_seen.insert(fresh_subject, now - Duration::from_secs(1));

        let evicted = sweep_stale_subjects(&mut per_subject, &mut last_seen, now, Duration::from_secs(5));

        assert_eq!(evicted, vec![stale_subject]);
        assert!(!per_subject.contains_key(&stale_subject), "stale subject's entry must be fully removed, not just trimmed");
        assert!(!last_seen.contains_key(&stale_subject));
        assert!(per_subject.contains_key(&fresh_subject), "fresh subject must be untouched");
        assert!(last_seen.contains_key(&fresh_subject));
    }

    #[test]
    fn build_silence_triples_substitutes_subject_placeholder() {
        let mut dict = Dictionary::new();
        let subject = dict.encode("http://example.org/sensor");

        let triples = build_silence_triples(
            "{subject} <http://example.org/status> \"silent\" .",
            subject,
            &mut dict,
        );

        assert_eq!(triples.len(), 1, "expected exactly one triple, got: {triples:?}");
        assert_eq!(dict.decode(triples[0].subject), Some("http://example.org/sensor"));
        assert_eq!(dict.decode(triples[0].predicate), Some("http://example.org/status"));
        assert_eq!(dict.decode(triples[0].object), Some(r#""silent""#));
    }

    #[test]
    fn build_silence_triples_warns_and_returns_empty_on_bad_template() {
        let mut dict = Dictionary::new();
        let subject = dict.encode("http://example.org/sensor");
        let triples = build_silence_triples("not valid rdf at all {subject}", subject, &mut dict);
        assert!(triples.is_empty());
    }

    // Real end-to-end wiring check for subject_timeout + silence_event: keeps
    // the capped_reader bridge open (unlike make_test_reader, which auto-closes)
    // so the background ticker thread gets a chance to actually fire while
    // `transform()` is still running. subject_timeout=0 means the very first
    // tick (internally every 250ms) evicts the subject; the 600ms wait is a
    // generous multiple of that to avoid flakiness from scheduling jitter.
    #[tokio::test]
    async fn subject_timeout_evicts_and_injects_silence_event() {
        // `run_capped_pass` only emits rule-*derived* facts, never raw ground
        // facts — so the rule set needs a rule that actually reacts to the
        // injected silence marker for it to show up in output at all.
        const RULES2: &str = r#"
@prefix ex: <http://example.org/> .
{ ?x ex:hasReading ?y } => { ?x ex:hasRecentReading ?y }
{ ?x ex:status ?s } => { ?x ex:reactedToStatus ?s }
"#;
        let (capped_reader, capped_bridge) = rdfc_processor_sdk::make_reader("mock://capped".to_string());
        let dummy_window = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [{"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"}],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": SINGLE_STREAM_QUERY,
                "rules": RULES2,
                "capped_reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://capped"},
                "max_per_subject": 5,
                "subject_timeout": 0,
                "silence_event": "{subject} <http://example.org/status> \"silent\" ."
            }),
            [("reader", dummy_window), ("capped", capped_reader)],
            [("writer", writer)],
        );

        capped_bridge
            .send(Bytes::from_static(
                b"<http://example.org/sensor> <http://example.org/hasReading> \"1\" .",
            ))
            .unwrap();

        let handle = tokio::spawn(async move { proc.transform().await });
        // `collect_writer` must run concurrently with the wait below, not just
        // after it: `writer.write_str().await` blocks until something drains
        // and ACKs the WriterBridge, so without an active reader `transform()`
        // would stall forever on its very first write, never reaching the
        // tick-processing code the rest of this test depends on.
        let collector = tokio::spawn(collect_writer(bridge));

        tokio::time::sleep(Duration::from_millis(600)).await;
        capped_bridge.close();

        let transform_result = handle.await.unwrap();
        transform_result.unwrap();
        let (received, _) = collector.await.unwrap();
        let text = String::from_utf8(received.concat()).unwrap();
        assert!(
            text.contains("reactedToStatus") && text.contains("\"silent\""),
            "expected a fact derived from the silence_event marker in output, got: {text}"
        );
    }

    #[tokio::test]
    async fn silence_event_without_subject_timeout_is_rejected() {
        let capped = make_test_reader("mock://capped", vec![]);
        let dummy_window = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [{"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"}],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": SINGLE_STREAM_QUERY,
                "rules": RULES,
                "capped_reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://capped"},
                "max_per_subject": 2,
                "silence_event": "{subject} <http://example.org/status> \"silent\" ."
            }),
            [("reader", dummy_window), ("capped", capped)],
            [("writer", writer)],
        );
        let (result, _) = tokio::join!(proc.transform(), collect_writer(bridge));
        assert!(result.is_err(), "expected an error, silence_event requires subject_timeout");
    }

    #[tokio::test]
    async fn subject_timeout_without_capped_reader_is_rejected() {
        let dummy_window = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Kolibrie = make_test_processor(
            json!({
                "readers": [{"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"}],
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rsp_query": SINGLE_STREAM_QUERY,
                "rules": RULES,
                "subject_timeout": 30
            }),
            [("reader", dummy_window)],
            [("writer", writer)],
        );
        let (result, _) = tokio::join!(proc.transform(), collect_writer(bridge));
        assert!(result.is_err(), "expected an error, subject_timeout requires capped_reader");
    }

    // ── RDF parser unit tests ──────────────────────────────────────────────────

    #[test]
    fn parses_uri_triple() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf("<http://s> <http://p> <http://o> .\n", &mut dict);
        assert_eq!(triples.len(), 1);
        assert_eq!(dict.decode(triples[0].subject), Some("http://s"));
        assert_eq!(dict.decode(triples[0].predicate), Some("http://p"));
        assert_eq!(dict.decode(triples[0].object), Some("http://o"));
    }

    #[test]
    fn parses_typed_literal() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(
            r#"<http://s> <http://p> "42"^^<http://www.w3.org/2001/XMLSchema#integer> ."#,
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        let o = dict.decode(triples[0].object).unwrap();
        assert!(o.contains("42"), "got: {o}");
    }

    #[test]
    fn parses_language_literal() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(r#"<http://s> <http://p> "hello"@en ."#, &mut dict);
        assert_eq!(triples.len(), 1);
        let o = dict.decode(triples[0].object).unwrap();
        assert_eq!(o, r#""hello"@en"#);
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(
            "# comment\n\n<http://s> <http://p> <http://o> .\n",
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
    }

    #[test]
    fn parses_turtle_with_prefixes() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(
            "@prefix ex: <http://example.org/> .\nex:s ex:p ex:o .\n",
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        assert_eq!(
            dict.decode(triples[0].subject),
            Some("http://example.org/s")
        );
    }

    #[test]
    fn parses_trig_named_graph_flattened_to_triple() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(
            "<http://g> { <http://s> <http://p> <http://o> . }",
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        assert_eq!(dict.decode(triples[0].subject), Some("http://s"));
    }

    #[test]
    fn parses_bare_nquads() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(
            "<http://s> <http://p> <http://o> <http://g> .\n",
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        assert_eq!(dict.decode(triples[0].subject), Some("http://s"));
    }

    #[test]
    fn rdf_to_ntriples_roundtrips_uri_triple() {
        let nt = rdf_to_ntriples("<http://s> <http://p> <http://o> .\n").unwrap();
        assert_eq!(nt, "<http://s> <http://p> <http://o> .\n");
    }

    #[test]
    fn nt_term_wraps_iri() {
        assert_eq!(nt_term("http://example.org/"), "<http://example.org/>");
    }

    #[test]
    fn nt_term_passes_through_literal() {
        assert_eq!(nt_term(r#""hello"@en"#), r#""hello"@en"#);
    }

    #[test]
    fn nt_term_passes_through_bnode() {
        assert_eq!(nt_term("_:b0"), "_:b0");
    }
}
