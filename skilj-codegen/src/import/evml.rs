//! Reads a model in the text language of [event-modeling-tools](https://github.com/lgazo/event-modeling-tools)
//! (`.evml` files, or Mermaid's `eventmodeling` diagram), by the naming
//! convention of `docs/modeling-tools.md`:
//!
//! - An event is in the bounded context its namespace names, in
//!   `snake_case`: `evt banking.MoneyDeposited` (or `Banking.`). Its name is
//!   its `NAME`.
//! - A command belongs to the bounded context of the events it leads to: the
//!   event's frame names it with `->>`, or follows it without one.
//! - Fields come from the frame's data, inline or a `data` block named with
//!   `[[...]]`, one `name: type` per line or comma: `string`, `uuid`, `date`
//!   and `datetime` are `string`, `integer`, `int` and `long` `i64`,
//!   `boolean` and `bool` `bool`. An example value works too (`'acc-1'`,
//!   `100`, `true`). `tag` after the type marks a DCB tag, `pii` personal
//!   data; `name?` is optional.
//! - A `ui` frame in the `api` namespace leading to a command makes it
//!   `rest_trigger_allowed`.
//! - A `note` on a command's frame with a line `reject <kind>: <reason>` is a
//!   rejection. Given/when/then has no way to say a command was refused.
//!
//! Only what the import needs is read; a model the language's own parser
//! accepts is read the same way, apart from an error it would report that
//! doesn't concern the import.

use super::{
    convert, is_type_name, no_events, rejection_kind, snake_case, FieldIn, Model, TypeOut,
};
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// An identifier, possibly qualified: `banking.MoneyDeposited`.
    Word(String),
    /// A frame number.
    Number(String),
    Arrow,
    OpenRef,
    CloseRef,
    /// A data type in backticks: `json`.
    DataType,
    /// The text between a data block's or inline data's braces.
    Data(String),
    Text(String),
}

struct Scanner {
    chars: Vec<char>,
    pos: usize,
    line: usize,
    tokens: Vec<(Token, usize)>,
}

fn error(line: usize, message: impl Into<String>) -> Error {
    Error::Evml {
        line,
        message: message.into(),
    }
}

impl Scanner {
    fn new(source: &str) -> Self {
        Scanner {
            chars: source.chars().collect(),
            pos: 0,
            line: 1,
            tokens: Vec::new(),
        }
    }

    fn peek(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.chars.get(self.pos).copied()?;
        self.pos += 1;
        if c == '\n' {
            self.line += 1;
        }
        Some(c)
    }

    fn rest_starts_with(&self, text: &str) -> bool {
        text.chars()
            .enumerate()
            .all(|(i, c)| self.peek(i) == Some(c))
    }

    fn at_line_start(&self) -> bool {
        self.chars[..self.pos]
            .iter()
            .rev()
            .take_while(|c| **c != '\n')
            .all(|c| *c == ' ' || *c == '\t')
    }

    fn skip_line(&mut self) {
        while let Some(c) = self.peek(0) {
            if c == '\n' {
                break;
            }
            self.bump();
        }
    }

    fn skip_until(&mut self, end: &str) {
        while self.peek(0).is_some() && !self.rest_starts_with(end) {
            self.bump();
        }
        for _ in end.chars() {
            self.bump();
        }
    }

    fn scan(mut self) -> Result<Vec<(Token, usize)>, Error> {
        while let Some(c) = self.peek(0) {
            let line = self.line;
            if c.is_whitespace() {
                self.bump();
            } else if self.rest_starts_with("%%{") {
                self.skip_until("}%%");
            } else if self.rest_starts_with("%%") || self.rest_starts_with("//") {
                self.skip_line();
            } else if self.rest_starts_with("/*") {
                self.skip_until("*/");
            } else if self.rest_starts_with("---") && self.at_line_start() {
                self.skip_line();
                loop {
                    if self.peek(0).is_none() {
                        break;
                    }
                    self.bump();
                    while matches!(self.peek(0), Some(' ' | '\t')) {
                        self.bump();
                    }
                    if self.rest_starts_with("---") {
                        self.skip_line();
                        break;
                    }
                    self.skip_line();
                }
            } else if self.rest_starts_with("->>") {
                self.pos += 3;
                self.tokens.push((Token::Arrow, line));
            } else if self.rest_starts_with("[[") {
                self.pos += 2;
                self.tokens.push((Token::OpenRef, line));
            } else if self.rest_starts_with("]]") {
                self.pos += 2;
                self.tokens.push((Token::CloseRef, line));
            } else if c == '`' {
                self.bump();
                while self.peek(0).is_some_and(|c| c != '`' && c != '\n') {
                    self.bump();
                }
                if self.bump() != Some('`') {
                    return Err(error(line, "a data type in backticks isn't closed"));
                }
                self.tokens.push((Token::DataType, line));
            } else if c == '"' || c == '\'' {
                self.bump();
                let mut text = String::new();
                loop {
                    match self.bump() {
                        Some(q) if q == c => break,
                        Some('\n') | None => {
                            return Err(error(line, "a quoted text isn't closed on its line"))
                        }
                        Some(other) => text.push(other),
                    }
                }
                self.tokens.push((Token::Text(text), line));
            } else if c == '{' {
                let data = self.data(line)?;
                self.tokens.push((Token::Data(data), line));
            } else if c.is_ascii_digit() {
                let mut number = String::new();
                while self.peek(0).is_some_and(|c| c.is_ascii_digit()) {
                    number.push(self.bump().unwrap_or_default());
                }
                self.tokens.push((Token::Number(number), line));
            } else if c == '_' || c.is_ascii_alphabetic() {
                let mut word = String::new();
                loop {
                    while self
                        .peek(0)
                        .is_some_and(|c| c == '_' || c.is_ascii_alphanumeric())
                    {
                        word.push(self.bump().unwrap_or_default());
                    }
                    if self.peek(0) == Some('.')
                        && self
                            .peek(1)
                            .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
                    {
                        word.push(self.bump().unwrap_or_default());
                    } else {
                        break;
                    }
                }
                if matches!(word.as_str(), "title" | "accTitle" | "accDescr") {
                    self.skip_title(&word);
                } else {
                    self.tokens.push((Token::Word(word), line));
                }
            } else {
                return Err(error(line, format!("unexpected `{c}`")));
            }
        }
        Ok(self.tokens)
    }

    /// `title ...`, `accTitle: ...` and `accDescr: ...` or `accDescr { ... }`.
    fn skip_title(&mut self, word: &str) {
        if word == "accDescr" {
            while matches!(self.peek(0), Some(' ' | '\t' | '\n' | '\r')) {
                self.bump();
            }
            if self.peek(0) == Some('{') {
                self.skip_until("}");
                return;
            }
        }
        self.skip_line();
    }

    /// Data from its `{`: a block when only blanks follow on its line, up to
    /// the first line that starts with `}`; inline otherwise, up to the last
    /// `}` on the line. The language's own rules, which never count braces.
    fn data(&mut self, line: usize) -> Result<String, Error> {
        let mut ahead = 1;
        while matches!(self.peek(ahead), Some(' ' | '\t')) {
            ahead += 1;
        }
        let block = match self.peek(ahead) {
            Some('\n') => true,
            Some('\r') => self.peek(ahead + 1) == Some('\n'),
            _ => false,
        };
        self.bump();
        let mut body = String::new();
        if block {
            loop {
                let Some(c) = self.bump() else {
                    return Err(error(line, "a data block isn't closed with a `}` line"));
                };
                body.push(c);
                if c == '\n' {
                    let mut ahead = 0;
                    while matches!(self.peek(ahead), Some(' ' | '\t')) {
                        ahead += 1;
                    }
                    if self.peek(ahead) == Some('}')
                        && self.peek(ahead + 1).is_none_or(|c| c.is_whitespace())
                    {
                        for _ in 0..=ahead {
                            self.bump();
                        }
                        return Ok(body);
                    }
                }
            }
        }
        let line_end = self.chars[self.pos..]
            .iter()
            .position(|c| *c == '\n')
            .map_or(self.chars.len(), |i| self.pos + i);
        let Some(close) = self.chars[self.pos..line_end]
            .iter()
            .rposition(|c| *c == '}')
            .map(|i| self.pos + i)
        else {
            return Err(error(line, "inline data isn't closed with `}` on its line"));
        };
        body.extend(&self.chars[self.pos..close]);
        self.pos = close + 1;
        Ok(body)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Ui,
    Processor,
    Command,
    ReadModel,
    Event,
}

fn kind(word: &str) -> Option<Kind> {
    Some(match word {
        "ui" => Kind::Ui,
        "pcr" | "processor" => Kind::Processor,
        "cmd" | "command" => Kind::Command,
        "rmo" | "readmodel" => Kind::ReadModel,
        "evt" | "event" => Kind::Event,
        _ => return None,
    })
}

struct Frame {
    id: String,
    line: usize,
    kind: Kind,
    namespace: Option<String>,
    name: String,
    reset: bool,
    sources: Vec<String>,
    data_ref: Option<String>,
    inline: Option<String>,
}

#[derive(Default)]
struct Parsed {
    frames: Vec<Frame>,
    data: BTreeMap<String, String>,
    notes: Vec<(String, String)>,
}

struct Parser {
    tokens: Vec<(Token, usize)>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(t, _)| t)
    }

    fn line(&self) -> usize {
        self.tokens
            .get(self.pos)
            .or(self.tokens.last())
            .map_or(1, |(_, l)| *l)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).map(|(t, _)| t.clone());
        self.pos += 1;
        token
    }

    fn word(&mut self, what: &str) -> Result<String, Error> {
        let line = self.line();
        match self.next() {
            Some(Token::Word(word)) => Ok(word),
            _ => Err(error(line, format!("expected {what}"))),
        }
    }

    fn number(&mut self, what: &str) -> Result<String, Error> {
        let line = self.line();
        match self.next() {
            Some(Token::Number(number)) if number.len() <= 3 => Ok(number),
            _ => Err(error(
                line,
                format!("expected {what}, a number of 1 to 3 digits"),
            )),
        }
    }

    /// Optional data: a data type in backticks, then a block or inline data
    /// (or, inline only, a quoted text).
    fn data(&mut self) -> Option<String> {
        let start = self.pos;
        if self.peek() == Some(&Token::DataType) {
            self.pos += 1;
        }
        match self.peek() {
            Some(Token::Data(data)) => {
                let data = data.clone();
                self.pos += 1;
                Some(data)
            }
            Some(Token::Text(text)) => {
                let text = text.clone();
                self.pos += 1;
                Some(text)
            }
            _ => {
                self.pos = start;
                None
            }
        }
    }

    fn parse(mut self) -> Result<Parsed, Error> {
        if self.word("`eventmodeling` first")? != "eventmodeling" {
            return Err(error(1, "a model starts with `eventmodeling`"));
        }
        let mut parsed = Parsed::default();
        while let Some(token) = self.next() {
            let line = self.tokens[self.pos - 1].1;
            let Token::Word(word) = token else {
                return Err(error(line, "expected a statement"));
            };
            match word.as_str() {
                "tf" | "timeframe" | "rf" | "resetframe" => {
                    let id = self.number("a frame number")?;
                    let kind_word = self.word("an entity type")?;
                    let kind = kind(&kind_word).ok_or_else(|| {
                        error(line, format!("`{kind_word}` is not an entity type"))
                    })?;
                    let identifier = self.word("an entity identifier")?;
                    let (namespace, name) = match identifier.rsplit_once('.') {
                        Some((namespace, name)) => (Some(namespace.to_string()), name.to_string()),
                        None => (None, identifier),
                    };
                    let mut sources = Vec::new();
                    while self.peek() == Some(&Token::Arrow) {
                        self.pos += 1;
                        sources.push(self.number("a source frame")?);
                    }
                    let mut data_ref = None;
                    if self.peek() == Some(&Token::OpenRef) {
                        self.pos += 1;
                        data_ref = Some(self.word("a data name")?);
                        if self.next() != Some(Token::CloseRef) {
                            return Err(error(line, "expected `]]`"));
                        }
                    }
                    let inline = self.data();
                    parsed.frames.push(Frame {
                        id,
                        line,
                        kind,
                        namespace,
                        name,
                        reset: word.starts_with('r'),
                        sources,
                        data_ref,
                        inline,
                    });
                }
                "entity" => {
                    self.word("an entity name")?;
                }
                "data" => {
                    let name = self.word("a data name")?;
                    let body = self
                        .data()
                        .ok_or_else(|| error(line, format!("data `{name}` has no block")))?;
                    parsed.data.insert(name, body);
                }
                "note" => {
                    let frame = self.number("a frame number")?;
                    let body = self
                        .data()
                        .ok_or_else(|| error(line, "a note has no block"))?;
                    parsed.notes.push((frame, body));
                }
                "gwt" => {
                    self.number("a frame number")?;
                    if matches!(self.peek(), Some(Token::Text(_))) {
                        self.pos += 1;
                    }
                    // Statements up to the next top-level keyword; nothing in
                    // them concerns the import.
                    while let Some(Token::Word(word)) = self.peek() {
                        match word.as_str() {
                            "given" | "when" | "then" => self.pos += 1,
                            w if kind(w).is_some() => {
                                self.pos += 1;
                                self.word("an entity identifier")?;
                                self.data();
                            }
                            _ => break,
                        }
                    }
                }
                other => return Err(error(line, format!("unexpected `{other}`"))),
            }
        }
        Ok(parsed)
    }
}

/// One field of a data body, or why it can't be one.
fn fields(body: &str) -> Vec<FieldIn<'static>> {
    let mut entries = Vec::new();
    let mut depth = 0i32;
    let mut quote = None;
    let mut current = String::new();
    for c in body.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '"' | '\'') => quote = Some(c),
            (None, '[' | '{') => depth += 1,
            (None, ']' | '}') => depth -= 1,
            (None, '\n' | ',' | ';') if depth == 0 => {
                entries.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    entries.push(current);
    entries
        .iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once(':')?;
            let key = key.trim().trim_matches(|c| c == '"' || c == '\'');
            let (name, optional) = match key.strip_suffix('?') {
                Some(name) => (name, true),
                None => (key, false),
            };
            let value = value.trim();
            // A list or nested object: left out by `convert`, whatever its type.
            let (ty, rest): (&'static str, &str) = if value.starts_with(['[', '{']) {
                ("String", "")
            } else if let Some(q) = value.chars().next().filter(|c| *c == '"' || *c == '\'') {
                let end = value[1..].find(q).map_or(value.len(), |i| i + 2);
                ("String", &value[end.min(value.len())..])
            } else {
                let (first, rest) = value.split_once(char::is_whitespace).unwrap_or((value, ""));
                (value_type(first), rest)
            };
            let flags: BTreeSet<&str> = rest.split_whitespace().collect();
            Some(FieldIn {
                name: name.to_string(),
                ty,
                list: value.starts_with('['),
                nested: value.starts_with('{'),
                optional,
                id: flags.contains("tag"),
                pii: flags.contains("pii"),
            })
        })
        .collect()
}

/// The specification type of a type word or an unquoted example value.
fn value_type(value: &str) -> &'static str {
    let value = value.trim_end_matches(',');
    match value.to_ascii_lowercase().as_str() {
        "string" | "text" => "String",
        "uuid" => "UUID",
        "date" => "Date",
        "datetime" => "DateTime",
        "integer" | "int" | "long" => "Int",
        "boolean" | "bool" | "true" | "false" => "Boolean",
        "number" | "float" | "double" => "Number",
        "decimal" => "Decimal",
        _ if value.parse::<i64>().is_ok() => "Int",
        _ if value.parse::<f64>().is_ok() => "Number",
        _ => "Custom",
    }
}

pub(super) fn read(source: &str, bounded_context: &str) -> Result<Model, Error> {
    let tokens = Scanner::new(source).scan()?;
    let parsed = Parser { tokens, pos: 0 }.parse()?;
    let mut notes = Vec::new();

    let by_id: BTreeMap<&str, &Frame> = parsed.frames.iter().map(|f| (f.id.as_str(), f)).collect();
    // A frame's sources: the ones it names, or else, for a time frame, the
    // frame before it.
    let sources = |index: usize| -> Vec<&Frame> {
        let frame = &parsed.frames[index];
        if !frame.sources.is_empty() {
            frame
                .sources
                .iter()
                .filter_map(|id| by_id.get(id.as_str()).copied())
                .collect()
        } else if !frame.reset && index > 0 {
            vec![&parsed.frames[index - 1]]
        } else {
            Vec::new()
        }
    };
    let data_of = |frames: &[&Frame]| -> Result<Option<String>, Error> {
        for frame in frames {
            if let Some(inline) = &frame.inline {
                return Ok(Some(inline.clone()));
            }
            if let Some(name) = &frame.data_ref {
                return match parsed.data.get(name) {
                    Some(body) => Ok(Some(body.clone())),
                    None => Err(error(frame.line, format!("no `data {name}` block"))),
                };
            }
        }
        Ok(None)
    };

    let mut contexts = BTreeSet::new();
    let mut event_frames: Vec<(String, Vec<&Frame>)> = Vec::new();
    let mut command_names = Vec::new();
    for (index, frame) in parsed.frames.iter().enumerate() {
        if frame.kind != Kind::Event {
            continue;
        }
        contexts.insert(
            frame
                .namespace
                .clone()
                .unwrap_or_else(|| "(no namespace)".into()),
        );
        if frame.namespace.as_deref().map(snake_case).as_deref() != Some(bounded_context) {
            continue;
        }
        match event_frames
            .iter_mut()
            .find(|(name, _)| *name == frame.name)
        {
            Some((_, frames)) => frames.push(frame),
            None => event_frames.push((frame.name.clone(), vec![frame])),
        }
        for source in sources(index) {
            if source.kind == Kind::Command && !command_names.contains(&source.name) {
                command_names.push(source.name.clone());
            }
        }
    }
    if event_frames.is_empty() {
        return Err(no_events(bounded_context, contexts));
    }

    let convert_frames = |name: &str, frames: &[&Frame], notes: &mut Vec<String>| {
        let first = frames[0];
        let qualified = match &first.namespace {
            Some(namespace) => format!("{namespace}.{name}"),
            None => name.to_string(),
        };
        let source_id = format!("{qualified} (frame {})", first.id);
        if !is_type_name(name) {
            notes.push(format!(
                "{source_id}: left out, its name isn't a PascalCase type name"
            ));
            return Ok(None);
        }
        let body = data_of(frames)?;
        let fields = body.as_deref().map(fields).unwrap_or_default();
        Ok::<_, Error>(convert(
            &source_id,
            name.to_string(),
            fields,
            "`tag`",
            notes,
        ))
    };

    let mut events = Vec::new();
    for (name, frames) in &event_frames {
        if let Some(out) = convert_frames(name, frames, &mut notes)? {
            events.push(out);
        }
    }

    let mut commands: Vec<TypeOut> = Vec::new();
    for name in &command_names {
        let indexes: Vec<usize> = parsed
            .frames
            .iter()
            .enumerate()
            .filter(|(_, f)| f.kind == Kind::Command && f.name == *name)
            .map(|(i, _)| i)
            .collect();
        let frames: Vec<&Frame> = indexes.iter().map(|i| &parsed.frames[*i]).collect();
        let Some(mut out) = convert_frames(name, &frames, &mut notes)? else {
            continue;
        };
        out.rest_trigger_allowed = indexes.iter().any(|i| {
            sources(*i).iter().any(|s| {
                s.kind == Kind::Ui
                    && s.namespace.as_deref().map(snake_case).as_deref() == Some("api")
            })
        });
        for (frame, body) in &parsed.notes {
            if !frames.iter().any(|f| f.id == *frame) {
                continue;
            }
            for line in body.lines() {
                let line = line.trim().trim_start_matches(['-', '*']).trim();
                if let Some(rejection) = line.strip_prefix("reject ") {
                    out.rejections
                        .insert(rejection_kind(rejection.trim()).to_string());
                }
            }
        }
        commands.push(out);
    }

    Ok(Model {
        events,
        commands,
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types(body: &str) -> Vec<(String, &'static str, bool, bool, bool)> {
        fields(body)
            .into_iter()
            .map(|f| (f.name, f.ty, f.id, f.optional, f.list || f.nested))
            .collect()
    }

    #[test]
    fn reads_types_examples_and_flags() {
        assert_eq!(
            types("\n  account_id: string tag\n  amount: integer\n  note?: string\n"),
            vec![
                ("account_id".into(), "String", true, false, false),
                ("amount".into(), "Int", false, false, false),
                ("note".into(), "String", false, true, false),
            ]
        );
        assert_eq!(
            types(" id: \"p-1\" tag, qty: 2, price: 20.4, paid: true, items: [ { a: 1 } ] "),
            vec![
                ("id".into(), "String", true, false, false),
                ("qty".into(), "Int", false, false, false),
                ("price".into(), "Number", false, false, false),
                ("paid".into(), "Boolean", false, false, false),
                ("items".into(), "String", false, false, true),
            ]
        );
        assert_eq!(
            types("\n\"name\": \"foo\"\n"),
            vec![("name".into(), "String", false, false, false)]
        );
    }

    #[test]
    fn data_blocks_end_at_the_first_closing_line() {
        let tokens = Scanner::new("eventmodeling\ndata A {\n  x: 1 }\n  y: 2\n}\n")
            .scan()
            .unwrap();
        assert_eq!(tokens[3].0, Token::Data("\n  x: 1 }\n  y: 2\n".into()));
        let tokens = Scanner::new("tf 01 cmd A { a: { b: 1 } } %% comment\n")
            .scan()
            .unwrap();
        assert_eq!(tokens[4].0, Token::Data(" a: { b: 1 } ".into()));
        assert_eq!(tokens.len(), 5);
    }
}
