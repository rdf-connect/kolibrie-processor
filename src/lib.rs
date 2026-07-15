//! Kolibrie reasoning processor for RDF-Connect.
//!
//! Reads RDF messages (Turtle, TriG, N-Triples or N-Quads — auto-detected,
//! no content-type needed) from `reader`, applies Datalog rules (supplied
//! inline or from a file) using the Kolibrie engine, and writes newly
//! derived facts as N-Triples to `writer`.
//!
//! ## Parameters
//!
//! | field          | type     | required | description                                             |
//! |----------------|----------|----------|---------------------------------------------------------|
//! | `reader`       | Reader   | yes      | Channel carrying RDF messages (Turtle/TriG/N-Triples/N-Quads) |
//! | `writer`       | Writer   | yes      | Channel to which derived N-Triples are written          |
//! | `rules`        | string   | *        | Inline N3 rules (required unless `rules_file` is given) |
//! | `rules_file`   | string   | *        | Path to a file containing N3 rules                      |
//! | `window_size`  | u64      | no       | Sliding-window width in seconds (0 = no windowing)      |
//! | `window_slide` | u64      | no       | Window slide interval in seconds (0 = every message)    |
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
//! Rules are loaded once at startup. Each IRI in a rule must either be
//! written in full (`<http://…>`) or use a declared `@prefix`.
//!
//! ## Windowing
//!
//! When `window_size > 0` the processor keeps only the triples received in the
//! last `window_size` seconds. After each batch (or after `window_slide`
//! seconds if that parameter is set) inference is re-run on the current window
//! contents.  Without windowing, triples accumulate forever and inference is
//! incremental (only newly derivable facts are emitted).

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rdfc_processor_sdk::{futures::StreamExt, Processor, Reader, Slot, Writer};

use datalog::parser_n3_logic::parse_n3_document;
use datalog::reasoning::Reasoner;
use shared::dataset_index::DatasetIndex;
use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::rule_index::RuleIndex;
use shared::triple::Triple;

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct Args {
    reader: Reader,
    writer: Writer,
    /// Inline N3 rules text.
    #[serde(default)]
    rules: Option<String>,
    /// Path to a file containing N3 rules.
    #[serde(default)]
    rules_file: Option<String>,
    /// Sliding-window width in seconds.  0 = no windowing (accumulate forever).
    #[serde(default)]
    window_size: u64,
    /// Slide interval in seconds.  0 = re-evaluate after every incoming message.
    #[serde(default)]
    window_slide: u64,
}

// ── Processor ─────────────────────────────────────────────────────────────────

pub struct Kolibrie {
    reader: Slot<Reader>,
    writer: Slot<Writer>,
    rules: Option<String>,
    rules_file: Option<String>,
    window_size: u64,
    window_slide: u64,
}

impl Processor for Kolibrie {
    type Args = Args;

    fn new(args: Args) -> Self {
        Kolibrie {
            reader: Slot::new(args.reader),
            writer: Slot::new(args.writer),
            rules: args.rules,
            rules_file: args.rules_file,
            window_size: args.window_size,
            window_slide: args.window_slide,
        }
    }

    async fn transform(&self) -> Result<(), String> {
        let reader = self.reader.take();
        let writer = self.writer.take();

        // ── 1. Load and parse rules ───────────────────────────────────────────
        let rules_text = load_rules(self.rules.clone(), self.rules_file.clone())?;

        // Use a temporary Reasoner solely to populate the shared dictionary
        // while parsing rule IRIs / prefixes.
        let mut seed_reasoner = Reasoner::new();
        let parsed_rules = parse_rules(&rules_text, &mut seed_reasoner)?;

        // Share the dictionary between all future per-step Reasoners so that
        // the u32 IDs encoded into the rules remain valid.
        let shared_dict: Arc<RwLock<Dictionary>> = Arc::clone(&seed_reasoner.dictionary);

        eprintln!("[kolibrie] loaded {} rule(s)", parsed_rules.len());

        // ── 2. Process stream ──────────────────────────────────────────────────
        if self.window_size == 0 {
            run_incremental(reader, writer, shared_dict, parsed_rules).await
        } else {
            run_windowed(
                reader,
                writer,
                shared_dict,
                parsed_rules,
                Duration::from_secs(self.window_size),
                Duration::from_secs(self.window_slide),
            )
            .await
        }
    }
}

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#KolibrieProcessor", Kolibrie);
rdfc_processor_sdk::rdfc_export_processors!();

// ── Incremental mode (no windowing) ──────────────────────────────────────────

/// Accumulate triples forever; after each batch emit only the facts newly
/// derivable by Kolibrie's semi-naive materialisation.
async fn run_incremental(
    reader: Reader,
    writer: Writer,
    shared_dict: Arc<RwLock<Dictionary>>,
    parsed_rules: Vec<Rule>,
) -> Result<(), String> {
    let mut reasoner = fresh_reasoner(&parsed_rules, &shared_dict);
    let mut stream = reader.into_stream();

    while let Some(bytes) = stream.next().await {
        let text = String::from_utf8_lossy(&bytes);
        let batch = parse_rdf(&text, &mut *shared_dict.write().unwrap());
        for triple in batch {
            reasoner.insert_ground_triple(triple);
        }

        // Returns only the facts derived in this call that were not already
        // present in the dataset index.
        let new_facts = reasoner.infer_new_facts_semi_naive();
        if !new_facts.is_empty() {
            let nt = triples_to_ntriples(&new_facts, &shared_dict.read().unwrap());
            writer.write_str(&nt).await.map_err(|e| e.to_string())?;
        }
    }

    writer.close();
    Ok(())
}

// ── Windowed mode ─────────────────────────────────────────────────────────────

/// Keep only triples received within the last `window_size`.  Re-run full
/// inference after every batch (or after `slide` has elapsed).  Emit only
/// facts not previously written (append-only, no retractions).
async fn run_windowed(
    reader: Reader,
    writer: Writer,
    shared_dict: Arc<RwLock<Dictionary>>,
    parsed_rules: Vec<Rule>,
    window_size: Duration,
    slide: Duration,
) -> Result<(), String> {
    // (arrived_at, triples)
    let mut window: VecDeque<(Instant, Vec<Triple>)> = VecDeque::new();
    // Facts already written — avoids re-emitting the same inferred fact.
    let mut written: HashSet<Triple> = HashSet::new();

    let mut last_slide = Instant::now();
    let mut stream = reader.into_stream();

    while let Some(bytes) = stream.next().await {
        let arrived = Instant::now();
        let text = String::from_utf8_lossy(&bytes);
        let batch = parse_rdf(&text, &mut *shared_dict.write().unwrap());
        if !batch.is_empty() {
            window.push_back((arrived, batch));
        }

        // Slide: skip evaluation if the slide interval hasn't elapsed yet.
        let should_evaluate = slide.is_zero() || arrived.duration_since(last_slide) >= slide;
        if !should_evaluate {
            continue;
        }
        last_slide = arrived;

        // Evict expired batches.
        while let Some(front) = window.front() {
            if arrived.duration_since(front.0) > window_size {
                window.pop_front();
            } else {
                break;
            }
        }

        // Build a fresh Reasoner from the current window contents.
        let mut reasoner = fresh_reasoner(&parsed_rules, &shared_dict);
        for (_, triples) in &window {
            for triple in triples {
                reasoner.insert_ground_triple(triple.clone());
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
            let nt = triples_to_ntriples(&new_facts, &shared_dict.read().unwrap());
            writer.write_str(&nt).await.map_err(|e| e.to_string())?;
        }
    }

    writer.close();
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a new [`Reasoner`] with rules loaded and using the shared dictionary.
fn fresh_reasoner(rules: &[Rule], shared_dict: &Arc<RwLock<Dictionary>>) -> Reasoner {
    let mut r = Reasoner {
        dictionary: Arc::clone(shared_dict),
        rules: Vec::new(),
        dataset_index: DatasetIndex::new(),
        rule_index: RuleIndex::new(),
        constraints: Vec::new(),
        probability_seeds: std::collections::HashMap::new(),
    };
    for rule in rules {
        // add_rule also updates rule_index; use try_add_rule to surface errors.
        if let Err(e) = r.try_add_rule(rule.clone()) {
            eprintln!("[kolibrie] warning: skipping rule: {e}");
        }
    }
    r
}

/// Resolve the rules text from either the inline parameter or a file.
fn load_rules(inline: Option<String>, file: Option<String>) -> Result<String, String> {
    match (inline, file) {
        (Some(text), _) => Ok(text),
        (None, Some(path)) => {
            std::fs::read_to_string(&path)
                .map_err(|e| format!("cannot read rules file '{path}': {e}"))
        }
        (None, None) => Err(
            "kolibrie processor: either 'rules' or 'rules_file' must be provided".to_string(),
        ),
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

// ── RDF parser (Turtle / TriG / N-Triples / N-Quads) ──────────────────────────

/// Parse an RDF document of unknown (but auto-detected) syntax into encoded
/// [`Triple`]s.
///
/// No content-type or format hint is required: we simply try each syntax in
/// turn and keep the first one that parses cleanly.
///
/// - [`TriGParser`] alone covers Turtle, TriG *and* N-Triples, since each of
///   those is a syntactic subset of TriG.
/// - The only shape it can't handle is "bare" N-Quads (`s p o g .` without a
///   `{ }` graph block), so [`NQuadsParser`] is tried as a fallback.
///
/// Named graphs (if any) are flattened away — Kolibrie's `Triple` has no
/// graph component, so quads are downgraded to plain triples.
///
/// Terms are stored in the dictionary in the following canonical form:
/// - IRI      → `http://example.org/foo`  (without angle brackets)
/// - Literal  → `"hello"` / `"42"^^<xsd:integer>` / `"hi"@en`  (with quotes)
/// - Blank    → `_:b0`
///
/// This matches how Kolibrie's N3 rule parser encodes IRIs so that rule
/// predicates and data predicates share the same dictionary IDs.
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

/// Try parsing `text` as Turtle/TriG/N-Triples. Returns `None` on the first
/// syntax error so the caller can fall back to another format.
fn parse_trig(text: &str, dict: &mut Dictionary) -> Option<Vec<Triple>> {
    let mut triples = Vec::new();
    for quad in oxttl::TriGParser::new().for_slice(text.as_bytes()) {
        triples.push(encode_quad(quad.ok()?, dict));
    }
    Some(triples)
}

/// Try parsing `text` as N-Quads (the one shape TriG's grammar doesn't cover).
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
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::make_writer;
    use serde_json::json;

    // Simple ancestry rules in N3 format.
    const RULES: &str = r#"
@prefix ex: <http://example.org/> .

{ ?x ex:parent ?y } => { ?x ex:ancestor ?y }
{ ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
"#;

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
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
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
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
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

    // ── RDF parser unit tests ──────────────────────────────────────────────────

    #[test]
    fn parses_uri_triple() {
        let mut dict = Dictionary::new();
        let triples = parse_rdf(
            "<http://s> <http://p> <http://o> .\n",
            &mut dict,
        );
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
        let triples = parse_rdf(
            r#"<http://s> <http://p> "hello"@en ."#,
            &mut dict,
        );
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
