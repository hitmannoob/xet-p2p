//! Built-in profiling, for finding where a transfer's time goes (also inside a sandbox).
//!
//! - `XET_P2P_TRACE=PATH`: records every span (ours and upstream's) from creation to close and, at
//!   exit, writes a Chrome trace (`PATH.<pid>.json`, open in <https://ui.perfetto.dev>) and a
//!   summary (`PATH.<pid>.txt`, also printed to stderr): per span name, how often it ran, its total
//!   and busy (polled) time, and its *coverage* (the share of the run during which at least one
//!   was active) and average concurrency. A stage with high coverage and concurrency near 1 is a
//!   serial bottleneck; one with low busy/total time is waiting on something.
//! - `XET_P2P_TRACE_LEVEL` sets which spans are recorded (default `debug` for this project's
//!   crates and `info` for upstream's).
//! - `XET_P2P_PROFILE=PATH` (builds with `--features cpu-profile`): samples CPU stacks at 999 Hz
//!   and writes a flame graph (`PATH.<pid>.svg`).
//!
//! `serve` writes its profile when it exits (idle exit, SIGTERM or Ctrl-C).

use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// One finished span.
struct Done {
    name: &'static str,
    target: &'static str,
    start_us: u64,
    end_us: u64,
    busy_us: u64,
}

/// Per-span state kept in the registry's extensions.
struct Timing {
    start: Instant,
    entered: Option<Instant>,
    busy_us: u64,
}

pub struct SpanRecorder {
    origin: Instant,
    done: Mutex<Vec<Done>>,
}

impl SpanRecorder {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            done: Mutex::new(Vec::new()),
        }
    }
}

impl<S> Layer<S> for &'static SpanRecorder
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Timing {
                start: Instant::now(),
                entered: None,
                busy_us: 0,
            });
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(t) = span.extensions_mut().get_mut::<Timing>()
        {
            t.entered = Some(Instant::now());
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(t) = span.extensions_mut().get_mut::<Timing>()
            && let Some(at) = t.entered.take()
        {
            t.busy_us += at.elapsed().as_micros() as u64;
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(t) = span.extensions_mut().remove::<Timing>() else {
            return;
        };
        let meta = span.metadata();
        let us = |i: Instant| i.saturating_duration_since(self.origin).as_micros() as u64;
        self.done.lock().unwrap().push(Done {
            name: meta.name(),
            target: meta.target(),
            start_us: us(t.start),
            end_us: us(Instant::now()),
            busy_us: t.busy_us,
        });
    }
}

/// Active profiling for this process; [`Profiling::finish`] writes the results.
pub struct Profiling {
    trace: Option<(String, &'static SpanRecorder)>,
    #[cfg(feature = "cpu-profile")]
    cpu: Option<(String, pprof::ProfilerGuard<'static>)>,
}

/// The span recorder layer (if `XET_P2P_TRACE` is set) and its per-layer filter.
pub fn layer<S>() -> (Option<impl Layer<S>>, Profiling)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    use tracing_subscriber::Layer as _;
    let pid = std::process::id();
    let trace = std::env::var("XET_P2P_TRACE").ok().map(|p| {
        let recorder: &'static SpanRecorder = Box::leak(Box::new(SpanRecorder::new()));
        (format!("{p}.{pid}"), recorder)
    });
    let filter = std::env::var("XET_P2P_TRACE_LEVEL").unwrap_or_else(|_| {
        "info,transfer=debug,xet_local=debug,transport_tcp=debug,transport_quic=debug,providers=debug"
            .into()
    });
    let layer = trace
        .as_ref()
        .map(|(_, r)| r.with_filter(tracing_subscriber::EnvFilter::new(filter)));
    #[cfg(feature = "cpu-profile")]
    let cpu = std::env::var("XET_P2P_PROFILE").ok().and_then(|p| {
        pprof::ProfilerGuardBuilder::default()
            .frequency(999)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .ok()
            .map(|g| (format!("{p}.{pid}"), g))
    });
    #[cfg(not(feature = "cpu-profile"))]
    if std::env::var_os("XET_P2P_PROFILE").is_some() {
        eprintln!("XET_P2P_PROFILE needs a build with `--features cpu-profile`; ignoring it");
    }
    (
        layer,
        Profiling {
            trace,
            #[cfg(feature = "cpu-profile")]
            cpu,
        },
    )
}

impl Profiling {
    /// Writes whatever was recorded. Safe to call once at exit.
    pub fn finish(self) {
        if let Some((path, recorder)) = self.trace {
            let wall_us = recorder.origin.elapsed().as_micros() as u64;
            let done = std::mem::take(&mut *recorder.done.lock().unwrap());
            if let Err(e) = write_trace(&path, &done) {
                eprintln!("profile: could not write {path}.json: {e}");
            }
            let summary = summarize(&done, wall_us);
            eprint!("{summary}");
            if let Err(e) = std::fs::write(format!("{path}.txt"), &summary) {
                eprintln!("profile: could not write {path}.txt: {e}");
            }
        }
        #[cfg(feature = "cpu-profile")]
        if let Some((path, guard)) = self.cpu {
            match guard.report().build() {
                Ok(report) => match std::fs::File::create(format!("{path}.svg")) {
                    Ok(f) if write_folded(&format!("{path}.folded"), &report).is_ok() => {
                        if let Err(e) = report.flamegraph(f) {
                            eprintln!("profile: flame graph failed: {e}");
                        } else {
                            eprintln!("profile: wrote {path}.svg");
                        }
                    }
                    Ok(_) => eprintln!("profile: could not write {path}.folded"),
                    Err(e) => eprintln!("profile: could not write {path}.svg: {e}"),
                },
                Err(e) => eprintln!("profile: no CPU report: {e}"),
            }
        }
    }
}

/// Folded stacks (`thread;outer;...;inner count`, one line per stack), for scripted analysis.
#[cfg(feature = "cpu-profile")]
fn write_folded(path: &str, report: &pprof::Report) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    for (frames, count) in &report.data {
        let mut line = frames.thread_name.replace(';', ":");
        for frame in frames.frames.iter().rev() {
            for sym in frame.iter().rev() {
                line.push(';');
                line.push_str(&sym.name().replace(';', ":"));
            }
        }
        writeln!(f, "{line} {count}")?;
    }
    f.flush()
}

/// Chrome trace event format: one async begin/end pair per span, grouped by target.
fn write_trace(path: &str, done: &[Done]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(format!("{path}.json"))?);
    f.write_all(b"[")?;
    for (i, d) in done.iter().enumerate() {
        let sep = if i == 0 { "" } else { "," };
        let name = serde_json::to_string(d.name).unwrap_or_default();
        let cat = serde_json::to_string(d.target).unwrap_or_default();
        write!(
            f,
            "{sep}{{\"name\":{name},\"cat\":{cat},\"ph\":\"b\",\"id\":{i},\"pid\":1,\"tid\":1,\"ts\":{}}},\
             {{\"name\":{name},\"cat\":{cat},\"ph\":\"e\",\"id\":{i},\"pid\":1,\"tid\":1,\"ts\":{},\"args\":{{\"busy_us\":{}}}}}",
            d.start_us, d.end_us, d.busy_us
        )?;
    }
    f.write_all(b"]")?;
    f.flush()
}

/// Per span name: count, total time, busy time, coverage of the run, average concurrency.
fn summarize(done: &[Done], wall_us: u64) -> String {
    let mut by: HashMap<(&str, &str), Vec<&Done>> = HashMap::new();
    for d in done {
        by.entry((d.target, d.name)).or_default().push(d);
    }
    let mut rows: Vec<_> = by
        .into_iter()
        .map(|((target, name), spans)| {
            let total: u64 = spans.iter().map(|d| d.end_us - d.start_us).sum();
            let busy: u64 = spans.iter().map(|d| d.busy_us).sum();
            // Union of the spans' intervals: how long at least one was active.
            let mut iv: Vec<(u64, u64)> = spans.iter().map(|d| (d.start_us, d.end_us)).collect();
            iv.sort_unstable();
            let (mut covered, mut cur) = (0u64, None::<(u64, u64)>);
            for (s, e) in iv {
                match cur {
                    Some((cs, ce)) if s <= ce => cur = Some((cs, ce.max(e))),
                    Some((cs, ce)) => {
                        covered += ce - cs;
                        cur = Some((s, e));
                    }
                    None => cur = Some((s, e)),
                }
            }
            if let Some((cs, ce)) = cur {
                covered += ce - cs;
            }
            (target, name, spans.len(), total, busy, covered)
        })
        .collect();
    rows.sort_by_key(|r| std::cmp::Reverse(r.5));
    let ms = |us: u64| us as f64 / 1000.0;
    let mut out = format!(
        "\nprofile: {} spans over {:.0} ms (coverage = share of the run with at least one active; \
         conc = total / covered)\n{:<44} {:>7} {:>11} {:>11} {:>9} {:>6}\n",
        done.len(),
        ms(wall_us),
        "span",
        "count",
        "total ms",
        "busy ms",
        "coverage",
        "conc"
    );
    for (target, name, count, total, busy, covered) in rows.into_iter().take(40) {
        let label = format!("{target}::{name}");
        let label = if label.len() > 44 {
            format!("…{}", &label[label.len() - 43..])
        } else {
            label
        };
        out += &format!(
            "{label:<44} {count:>7} {:>11.1} {:>11.1} {:>8.1}% {:>6.2}\n",
            ms(total),
            ms(busy),
            100.0 * covered as f64 / wall_us.max(1) as f64,
            total as f64 / covered.max(1) as f64
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_reports_coverage_and_concurrency() {
        let d = |s, e| Done {
            name: "stage",
            target: "t",
            start_us: s,
            end_us: e,
            busy_us: (e - s) / 2,
        };
        // Two overlapping spans over [0, 300) and one alone over [500, 600): covered 400 of 1000.
        let s = summarize(&[d(0, 200), d(100, 300), d(500, 600)], 1000);
        let line = s.lines().find(|l| l.starts_with("t::stage")).unwrap();
        assert!(line.contains("40.0%"), "{line}");
        assert!(line.ends_with("1.25"), "{line}");
    }
}
