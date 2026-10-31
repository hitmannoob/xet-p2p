//! Transfer targets: `scheme://authority/path[?options]`.
//!
//! - `quic://host:port/rel/path`, `tcp://host:port/rel/path` — an already running `serve`
//!   (the generic host adapter); the path is relative to that server's `--root`.
//! - `<provider>://SANDBOX_ID/abs/path` — an existing sandbox of a provider; the path is
//!   absolute inside the sandbox and must lie under the adapter's workspace root.
//!
//! Parsing is strict: no userinfo, no fragments, no `..`/`.`/empty segments, no backslashes or
//! NUL, and sandbox IDs limited to `[A-Za-z0-9._-]`.

use std::collections::BTreeMap;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A server reached directly at `host:port` (no provider involved).
    Direct {
        transport: DirectTransport,
        host_port: String,
        /// Relative to the server's root (`""` is not allowed for transfers).
        path: String,
    },
    /// A sandbox managed by a provider.
    Sandbox {
        provider: String,
        id: String,
        /// Absolute path inside the sandbox.
        path: String,
        options: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectTransport {
    Quic,
    Tcp,
}

impl Target {
    pub fn parse(s: &str) -> Result<Self> {
        let bad = |why: &str| Error::BadUri(format!("{s:?}: {why}"));
        let (scheme, rest) = s
            .split_once("://")
            .ok_or_else(|| bad("expected scheme://..."))?;
        if scheme.is_empty()
            || !scheme
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(bad("invalid scheme"));
        }
        if rest.contains('#') {
            return Err(bad("fragments are not allowed"));
        }
        let (rest, query) = match rest.split_once('?') {
            Some((r, q)) => (r, Some(q)),
            None => (rest, None),
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        if authority.is_empty() {
            return Err(bad("missing host or sandbox id"));
        }
        if authority.contains('@') {
            return Err(bad("credentials in the URI are not allowed"));
        }
        let segments = check_path(path).map_err(bad)?;
        let options = parse_query(query).map_err(bad)?;

        match scheme {
            "quic" | "tcp" => {
                if !options.is_empty() {
                    return Err(bad("direct targets take no options"));
                }
                if !authority.contains(':') {
                    return Err(bad("direct targets need host:port"));
                }
                Ok(Target::Direct {
                    transport: if scheme == "quic" {
                        DirectTransport::Quic
                    } else {
                        DirectTransport::Tcp
                    },
                    host_port: authority.to_owned(),
                    path: segments.join("/"),
                })
            }
            provider => {
                if !authority
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
                    || authority.len() > 128
                {
                    return Err(bad(
                        "sandbox ids may contain only letters, digits, '.', '_', '-'",
                    ));
                }
                Ok(Target::Sandbox {
                    provider: provider.to_owned(),
                    id: authority.to_owned(),
                    path: format!("/{}", segments.join("/")),
                    options,
                })
            }
        }
    }

    /// The path a transfer addresses, for messages.
    pub fn path(&self) -> &str {
        match self {
            Target::Direct { path, .. } | Target::Sandbox { path, .. } => path,
        }
    }
}

fn check_path(path: &str) -> std::result::Result<Vec<String>, &'static str> {
    if path.is_empty() || path == "/" {
        return Ok(Vec::new());
    }
    let path = path.strip_prefix('/').expect("path starts with '/'");
    if path.contains('\\') || path.contains('\0') {
        return Err("backslashes and NUL are not allowed in paths");
    }
    let mut out = Vec::new();
    for seg in path.split('/') {
        match seg {
            "" => return Err("empty path segment"),
            "." | ".." => return Err("relative path segments are not allowed"),
            s if s.contains('%') => return Err("percent-encoding is not supported"),
            s => out.push(s.to_owned()),
        }
    }
    Ok(out)
}

fn parse_query(q: Option<&str>) -> std::result::Result<BTreeMap<String, String>, &'static str> {
    let mut out = BTreeMap::new();
    let Some(q) = q else { return Ok(out) };
    for pair in q.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k.is_empty() || !k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err("invalid option name");
        }
        if out.insert(k.to_owned(), v.to_owned()).is_some() {
            return Err("duplicate option");
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_direct_and_sandbox_targets() {
        assert_eq!(
            Target::parse("quic://host.example:4433/proj/sub").unwrap(),
            Target::Direct {
                transport: DirectTransport::Quic,
                host_port: "host.example:4433".into(),
                path: "proj/sub".into()
            }
        );
        assert_eq!(Target::parse("tcp://10.0.0.1:9/").unwrap().path(), "");
        let t = Target::parse("modal://sb-AbC_1.2/workspace/project?udp=off").unwrap();
        assert_eq!(
            t,
            Target::Sandbox {
                provider: "modal".into(),
                id: "sb-AbC_1.2".into(),
                path: "/workspace/project".into(),
                options: [("udp".to_string(), "off".to_string())].into()
            }
        );
        assert_eq!(Target::parse("e2b://x").unwrap().path(), "/");
    }

    #[test]
    fn rejects_unsafe_or_malformed_targets() {
        for s in [
            "no-scheme",
            "://x/y",
            "Modal://x/y",
            "modal:///path",
            "modal://x/../etc",
            "modal://x/a/./b",
            "modal://x/a//b",
            "modal://x/a%2e%2e",
            "modal://x/a\\b",
            "modal://user@x/a",
            "modal://x/a#frag",
            "modal://bad id/a",
            "modal://x/a?=v",
            "modal://x/a?k=1&k=2",
            "quic://hostonly/a",
            "quic://h:1/a?opt=1",
        ] {
            assert!(Target::parse(s).is_err(), "{s}");
        }
    }
}
