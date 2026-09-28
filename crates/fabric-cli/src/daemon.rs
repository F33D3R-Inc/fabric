//! `fabric daemon ...`: a running fabricd, over its operator port.
//!
//! The operator port (fabric-daemon's `admin.rs`) is plain HTTP on a private
//! address, authenticated by the `x-api-key` header. This module is its
//! client -- `GET /status`, `GET /placements`, `POST /actions` -- written on
//! `std::net` alone: the port speaks HTTP/1.1 and nothing else, and a
//! blocking request and its answer is all a command-line tool needs.
//!
//! The token is read from `FABRIC_ADMIN_TOKEN` and nowhere else, so it never
//! appears on a command line; the port is `--admin` or `FABRIC_ADMIN_URL`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::Value;

use crate::args::{DaemonCommand, DaemonOp};

/// What a command answers: its output, and its exit status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub stdout: String,
    pub stderr: String,
    pub code: u8,
}

const EXIT_OK: u8 = 0;
const EXIT_ERROR: u8 = 1;
const EXIT_USAGE: u8 = 2;

pub const HELP: &str = "\
fabric daemon -- a running fabricd, over its operator port

USAGE:
    fabric daemon status      [--admin <url>] [--json]
    fabric daemon placements  [--admin <url>] [--json]
    fabric daemon migrate <shard> <x> <y> <destination> [--admin <url>]

COMMANDS:
    status      The daemon's snapshot: its backends, placements, the actions
                in flight and what its control loop decided
    placements  Which instance holds each cell, and any migration under way
    migrate     Ask for a cell to move to the instance <destination>; the
                daemon admits it through the same checks as its own decisions,
                or says why not

OPTIONS:
        --admin <url>  The operator port, http://host:port (default:
                       FABRIC_ADMIN_URL)
        --json         Print the daemon's JSON instead of a table
    -h, --help         Show help

The token is FABRIC_ADMIN_TOKEN, read from nowhere else: it never appears on
a command line.
";

fn usage(message: &str) -> Outcome {
    Outcome {
        stdout: String::new(),
        stderr: format!("error: {message}\ntry 'fabric daemon --help'\n"),
        code: EXIT_USAGE,
    }
}

fn failed(message: &str) -> Outcome {
    Outcome {
        stdout: String::new(),
        stderr: format!("error: {message}\n"),
        code: EXIT_ERROR,
    }
}

fn printed(stdout: String) -> Outcome {
    Outcome {
        stdout,
        stderr: String::new(),
        code: EXIT_OK,
    }
}

/// `host:port` of an `http://host:port` URL (a trailing `/` allowed), or
/// `None` for anything else -- the operator port has no TLS and no path.
pub fn authority(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("http://")?;
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    let (host, port) = rest.rsplit_once(':')?;
    if host.is_empty() || rest.contains('/') || port.parse::<u16>().is_err() {
        return None;
    }
    Some(rest)
}

/// One request to the operator port: its status and body, or `None` when
/// nothing answered.
fn exchange(authority: &str, method: &str, path: &str, token: &str, body: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(authority).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok()?;

    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nx-api-key: {token}\r\nConnection: close\r\n"
    );
    if method == "POST" {
        request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).ok()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    parse_response(&raw)
}

/// An HTTP/1.1 response read to the connection's close: the status, and the
/// body framed by Content-Length, chunked, or the close itself.
pub fn parse_response(raw: &[u8]) -> Option<(u16, String)> {
    let head_end = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&raw[..head_end]).ok()?;
    let mut lines = head.split("\r\n");
    let status: u16 = lines.next()?.split(' ').nth(1)?.parse().ok()?;
    let mut length: Option<usize> = None;
    let mut chunked = false;
    for line in lines {
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            length = Some(value.parse().ok()?);
        }
        if name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked") {
            chunked = true;
        }
    }
    let mut body = &raw[head_end + 4..];
    let mut out = Vec::new();
    if chunked {
        loop {
            let line_end = body.windows(2).position(|w| w == b"\r\n")?;
            let size = usize::from_str_radix(std::str::from_utf8(&body[..line_end]).ok()?.split(';').next()?.trim(), 16).ok()?;
            body = &body[line_end + 2..];
            if size == 0 {
                break;
            }
            out.extend_from_slice(body.get(..size)?);
            body = body.get(size + 2..)?;
        }
    } else if let Some(length) = length {
        out.extend_from_slice(body.get(..length)?);
    } else {
        out.extend_from_slice(body);
    }
    Some((status, String::from_utf8(out).ok()?))
}

/// Run one `fabric daemon` command against the environment it was given.
pub fn run(command: &DaemonCommand, admin_env: Option<String>, token_env: Option<String>) -> Outcome {
    if command.op == DaemonOp::Help {
        return printed(HELP.to_string());
    }

    let Some(url) = command.admin.clone().or(admin_env).filter(|url| !url.is_empty()) else {
        return usage("no operator port: pass --admin <url> or set FABRIC_ADMIN_URL");
    };
    let Some(authority) = authority(&url) else {
        return usage(&format!("the operator port must be http://host:port, got '{url}'"));
    };
    let Some(token) = token_env.filter(|token| !token.is_empty()) else {
        return failed("FABRIC_ADMIN_TOKEN is not set: the operator port is never used without its token");
    };

    let (method, path, body) = match &command.op {
        DaemonOp::Status => ("GET", "/status", String::new()),
        DaemonOp::Placements => ("GET", "/placements", String::new()),
        DaemonOp::Migrate { shard, x, y, destination } => (
            "POST",
            "/actions",
            serde_json::json!({ "shard": shard, "x": x, "y": y, "destination": destination }).to_string(),
        ),
        DaemonOp::Help => unreachable!("answered above"),
    };

    let Some((status, answer)) = exchange(authority, method, path, &token, &body) else {
        return failed(&format!("could not reach the operator port at {url}"));
    };

    match status {
        401 => failed("the operator port refused FABRIC_ADMIN_TOKEN"),

        200 if matches!(command.op, DaemonOp::Migrate { .. }) => printed(answer),

        409 if matches!(command.op, DaemonOp::Migrate { .. }) => Outcome {
            stdout: String::new(),
            stderr: format!("refused: {answer}"),
            code: EXIT_ERROR,
        },

        200 => {
            let Ok(value) = serde_json::from_str::<Value>(&answer) else {
                return failed("the operator port answered something that is not its JSON");
            };
            if command.json {
                return printed(format!("{}\n", serde_json::to_string_pretty(&value).expect("a Value serializes")));
            }
            match command.op {
                DaemonOp::Status => printed(render_status(&value)),
                _ => printed(render_placements(value.as_array().map(Vec::as_slice).unwrap_or(&[]))),
            }
        }

        other => failed(&format!("the operator port answered {other}: {}", answer.trim_end())),
    }
}

fn text(value: &Value, key: &str) -> String {
    match &value[key] {
        Value::String(s) => s.clone(),
        Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

/// The placements, one row per cell.
pub fn render_placements(placements: &[Value]) -> String {
    if placements.is_empty() {
        return "no placements\n".to_string();
    }
    let mut out = format!("{:<8}{:<10}{:<18}{:<12}{}\n", "SHARD", "CELL", "HOLDER", "REGION", "MIGRATION");
    for p in placements {
        let migration = match &p["migration"] {
            Value::Object(_) => {
                let m = &p["migration"];
                let mut shown = format!("{} {} -> {}", text(m, "phase"), text(m, "source"), text(m, "destination"));
                if m["write_fenced"] == Value::Bool(true) {
                    shown.push_str(" (writes fenced)");
                }
                shown
            }
            _ => "-".to_string(),
        };
        out.push_str(&format!(
            "{:<8}{:<10}{:<18}{:<12}{}\n",
            text(p, "shard"),
            format!("({},{})", text(p, "x"), text(p, "y")),
            text(p, "holder"),
            text(p, "region"),
            migration
        ));
    }
    out
}

/// The daemon's snapshot: who it is, its backends, its placements, what is
/// in flight and what its loop decided.
pub fn render_status(status: &Value) -> String {
    let mut out = format!(
        "fabricd {}: data {}, operator {}{}\n",
        text(status, "version"),
        text(status, "data_listen"),
        text(status, "admin_listen"),
        if status["draining"] == Value::Bool(true) { ", draining" } else { "" }
    );
    out.push_str(&format!(
        "control loop: {} cycle(s), routing generation {}, placement generation {}\n",
        text(status, "cycles"),
        text(status, "routing_generation"),
        text(status, "placement_generation")
    ));
    out.push_str(&format!("telemetry: {}\n\n", text(status, "telemetry_source")));

    let backends = status["backends"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    out.push_str(&format!("{:<18}{:<12}{:<14}{:<11}{}\n", "BACKEND", "REGION", "AVAILABILITY", "HEALTH", "LAST PROBE"));
    for b in backends {
        out.push_str(&format!(
            "{:<18}{:<12}{:<14}{:<11}{}\n",
            text(b, "id"),
            text(b, "region"),
            text(b, "availability"),
            text(b, "health"),
            text(b, "last_probe")
        ));
    }
    if backends.is_empty() {
        out.push_str("no backends\n");
    }

    out.push('\n');
    out.push_str(&render_placements(status["placements"].as_array().map(Vec::as_slice).unwrap_or(&[])));

    out.push('\n');
    let in_flight = status["in_flight"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    if in_flight.is_empty() {
        out.push_str("no action in flight\n");
    } else {
        out.push_str(&format!("{:<12}{:<11}{:<16}{:<34}{:<12}{}\n", "ACTION", "KIND", "TARGET", "FROM -> TO", "STATE", "PHASE"));
        for a in in_flight {
            out.push_str(&format!(
                "{:<12}{:<11}{:<16}{:<34}{:<12}{}\n",
                format!("action-{}", text(a, "id")),
                text(a, "action"),
                format!("shard {} ({},{})", text(a, "shard"), text(a, "x"), text(a, "y")),
                format!("{} -> {}", text(a, "source"), text(a, "destination")),
                text(a, "state"),
                text(a, "phase")
            ));
        }
    }

    let d = &status["decisions"];
    out.push_str(&format!(
        "decisions: {} proposed, {} admitted, {} refused\n",
        text(d, "proposed"),
        text(d, "admitted"),
        text(d, "refused")
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_plain_http_authority_is_an_operator_port() {
        assert_eq!(authority("http://127.0.0.1:7071"), Some("127.0.0.1:7071"));
        assert_eq!(authority("http://127.0.0.1:7071/"), Some("127.0.0.1:7071"));
        assert_eq!(authority("https://127.0.0.1:7071"), None);
        assert_eq!(authority("http://127.0.0.1"), None);
        assert_eq!(authority("http://127.0.0.1:7071/status"), None);
        assert_eq!(authority("http://:7071"), None);
    }

    #[test]
    fn a_response_is_read_by_its_framing() {
        assert_eq!(
            parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\nabcdef"),
            Some((200, "abc".to_string()))
        );
        assert_eq!(
            parse_response(b"HTTP/1.1 409 Conflict\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"),
            Some((409, "abcde".to_string()))
        );
        assert_eq!(parse_response(b"HTTP/1.1 200 OK\r\n\r\nto the close"), Some((200, "to the close".to_string())));
        assert_eq!(parse_response(b"not http"), None);
    }

    #[test]
    fn the_token_is_never_optional_and_the_port_never_guessed() {
        let status = DaemonCommand { op: DaemonOp::Status, admin: None, json: false };
        assert_eq!(run(&status, None, Some("t".into())).code, EXIT_USAGE);
        let with_port = DaemonCommand { admin: Some("http://127.0.0.1:1".into()), ..status };
        let missing = run(&with_port, None, None);
        assert_eq!(missing.code, EXIT_ERROR);
        assert!(missing.stderr.contains("FABRIC_ADMIN_TOKEN is not set"));
        let unreachable = run(&with_port, None, Some("t".into()));
        assert_eq!(unreachable.stderr, "error: could not reach the operator port at http://127.0.0.1:1\n");
    }

    #[test]
    fn placements_render_one_row_per_cell() {
        let placements: Value = serde_json::from_str(
            r#"[{"shard":1,"x":0,"y":0,"holder":"db-a","region":"us-east","migration":null},
                {"shard":1,"x":1,"y":0,"holder":"db-a","region":"us-east","migration":{"phase":"cutover","source":"db-a","destination":"db-b","read_owner":"db-a","write_fenced":true,"has_cut_over":false}}]"#,
        )
        .unwrap();
        assert_eq!(
            render_placements(placements.as_array().unwrap()),
            "SHARD   CELL      HOLDER            REGION      MIGRATION\n\
             1       (0,0)     db-a              us-east     -\n\
             1       (1,0)     db-a              us-east     cutover db-a -> db-b (writes fenced)\n"
        );
        assert_eq!(render_placements(&[]), "no placements\n");
    }
}
