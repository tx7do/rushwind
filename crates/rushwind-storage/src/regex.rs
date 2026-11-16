//! A hand-written, dependency-free regular-expression engine — the
//! contract's regex evaluation story (`Op::Regexp`/`Op::Iregexp`).
//! The contract layer stays dependency-free by doctrine, so the
//! engine is written here: a recursive-descent parser over a
//! documented subset, a Thompson NFA construction, and a linear-time
//! simulation — the RE2-class guarantee (no backtracking blowup; the
//! match cost is input length × pattern size, always).
//!
//! # The subset
//!
//! Literals, `.`, character classes with ranges and negation
//! (`[a-z]`, `[^0-9]`), the anchors `^`/`$` (string start/end — the
//! match itself is unanchored, RE2-style contains semantics),
//! quantifiers `*` `+` `?` `{m}` `{m,n}` `{m,}` (greedy, but greedy-
//! versus-lazy is unobservable to a boolean match), alternation `|`,
//! and groups `(...)` (match-only — no captures, so no
//! backreferences). Escapes carry the metacharacters through
//! (`\.` `\*` `\\` …); the shorthands (`\d`, `\w`) and the
//! word-boundary family refuse at parse time with their names in the
//! error — the subset renders identically across the engine fleet's
//! dialects (ICU, RE2, PCRE), which is what makes the push-down faces
//! honest.
//!
//! # Case folding
//!
//! `IREGEXP` folds the pattern and the input with the same
//! `to_lowercase` folding `Iexact` uses, then runs the case-sensitive
//! machine — classes fold endpoint-wise (`[A-Z]` becomes `[a-z]`).

/// One parse error with its byte position.
#[derive(Debug, Clone, PartialEq)]
pub struct RegexError {
    /// The byte offset into the pattern.
    pub position: usize,
    /// The reason (the refused construct's name or a syntax fault).
    pub message: String,
}

/// The parsed pattern's AST — public so the engines that cannot ride
/// the pattern verbatim (Elasticsearch's Lucene dialect has no
/// `^`/`$` anchors) can re-render it per top-level alternative.
#[derive(Debug, Clone, PartialEq)]
pub enum Ast {
    /// The empty pattern (matches everything).
    Empty,
    /// One literal character.
    Char(char),
    /// Any character (`.`).
    Any,
    /// A character class; `negated` inverts membership.
    Class {
        /// The class's ranges (inclusive).
        ranges: Vec<(char, char)>,
        /// Whether the class negates its ranges.
        negated: bool,
    },
    /// A concatenation (in order).
    Concat(Vec<Ast>),
    /// An alternation (any branch).
    Alt(Vec<Ast>),
    /// A repetition; `max = None` reads unbounded.
    Repeat {
        /// The repeated node.
        node: Box<Ast>,
        /// The minimum count.
        min: u32,
        /// The maximum count; `None` is unbounded.
        max: Option<u32>,
    },
    /// The string-start anchor (`^`).
    Start,
    /// The string-end anchor (`$`).
    End,
}

impl Ast {
    /// The pattern's top-level alternatives (a single branch reads as
    /// one alternative — the re-render faces walk these).
    pub fn top_level_alternatives(&self) -> Vec<&Ast> {
        match self {
            Ast::Alt(branches) => branches.iter().collect(),
            other => vec![other],
        }
    }
}

/// Parses one pattern into its AST.
pub fn parse(pattern: &str) -> Result<Ast, RegexError> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut parser = Parser { chars, pos: 0 };
    let ast = parser.alternation()?;
    if parser.pos != parser.chars.len() {
        return Err(parser.error("unexpected ')'"));
    }
    Ok(ast)
}

/// Whether `pattern` matches anywhere in `text` (the unanchored,
/// contains semantics the contract's `Regexp` carries). `fold` runs
/// the case-insensitive face (the `Iexact` folding).
pub fn is_match(ast: &Ast, text: &str, fold: bool) -> bool {
    let machine_ast = if fold { fold_ast(ast) } else { ast.clone() };
    let nfa = Nfa::build(&machine_ast);
    let input: Vec<char> = if fold {
        text.to_lowercase().chars().collect()
    } else {
        text.chars().collect()
    };
    nfa.matches(&input)
}

/// Folds the AST's literals and class endpoints (the `Iexact`
/// folding, applied pattern-side).
fn fold_ast(ast: &Ast) -> Ast {
    match ast {
        Ast::Empty => Ast::Empty,
        Ast::Char(c) => Ast::Char(fold_char(*c)),
        Ast::Any => Ast::Any,
        Ast::Class { ranges, negated } => Ast::Class {
            ranges: ranges
                .iter()
                .map(|(low, high)| (fold_char(*low), fold_char(*high)))
                .collect(),
            negated: *negated,
        },
        Ast::Concat(nodes) => Ast::Concat(nodes.iter().map(fold_ast).collect()),
        Ast::Alt(branches) => Ast::Alt(branches.iter().map(fold_ast).collect()),
        Ast::Repeat { node, min, max } => Ast::Repeat {
            node: Box::new(fold_ast(node)),
            min: *min,
            max: *max,
        },
        Ast::Start => Ast::Start,
        Ast::End => Ast::End,
    }
}

/// One character's folding — the simple code-point lowercase.
fn fold_char(c: char) -> char {
    c.to_lowercase().next().unwrap_or(c)
}

// ---- the parser ---------------------------------------------------------

struct Parser {
    chars: Vec<char>,
    pos: usize,
}

impl Parser {
    fn error(&self, message: &str) -> RegexError {
        RegexError {
            position: self.pos,
            message: message.to_string(),
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn alternation(&mut self) -> Result<Ast, RegexError> {
        let first = self.concat()?;
        if !self.eat('|') {
            return Ok(first);
        }
        let mut branches = vec![first];
        loop {
            branches.push(self.concat()?);
            if !self.eat('|') {
                break;
            }
        }
        Ok(Ast::Alt(branches))
    }

    fn concat(&mut self) -> Result<Ast, RegexError> {
        let mut nodes = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            nodes.push(self.repeat()?);
        }
        Ok(match nodes.len() {
            0 => Ast::Empty,
            1 => nodes.pop().expect("len checked"),
            _ => Ast::Concat(nodes),
        })
    }

    fn repeat(&mut self) -> Result<Ast, RegexError> {
        let atom = self.atom()?;
        let (min, max) = match self.peek() {
            Some('*') => {
                self.pos += 1;
                (0, None)
            }
            Some('+') => {
                self.pos += 1;
                (1, None)
            }
            Some('?') => {
                self.pos += 1;
                (0, Some(1))
            }
            Some('{') => match self.bounds()? {
                Some(bounds) => bounds,
                None => return Ok(atom),
            },
            _ => return Ok(atom),
        };
        // A double quantifier (`a**`) has no meaning in the subset.
        if matches!(self.peek(), Some('*' | '+' | '?' | '{')) {
            return Err(self.error("nested quantifiers"));
        }
        // An anchor never repeats.
        if matches!(atom, Ast::Start | Ast::End) {
            return Err(self.error("repeated anchor"));
        }
        Ok(Ast::Repeat {
            node: Box::new(atom),
            min,
            max,
        })
    }

    /// `{m}`, `{m,n}`, `{m,}` — but a `{` that is not a quantifier
    /// reads as a literal brace (the POSIX concession), so `None`
    /// backs the caller out.
    fn bounds(&mut self) -> Result<Option<(u32, Option<u32>)>, RegexError> {
        let start = self.pos;
        self.pos += 1; // '{'
        let mut low = String::new();
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            low.push(self.chars[self.pos]);
            self.pos += 1;
        }
        if low.is_empty() {
            self.pos = start;
            return Ok(None);
        }
        let min: u32 = low.parse().map_err(|_| self.error("quantifier overflow"))?;
        if self.eat('}') {
            return Ok(Some((min, Some(min))));
        }
        if !self.eat(',') {
            self.pos = start;
            return Ok(None);
        }
        let mut high = String::new();
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            high.push(self.chars[self.pos]);
            self.pos += 1;
        }
        if !self.eat('}') {
            self.pos = start;
            return Ok(None);
        }
        if high.is_empty() {
            return Ok(Some((min, None)));
        }
        let max: u32 = high
            .parse()
            .map_err(|_| self.error("quantifier overflow"))?;
        if max < min {
            return Err(self.error("quantifier max below min"));
        }
        Ok(Some((min, Some(max))))
    }

    fn atom(&mut self) -> Result<Ast, RegexError> {
        let start = self.pos;
        let Some(c) = self.peek() else {
            return Err(self.error("unexpected end of pattern"));
        };
        match c {
            '(' => {
                self.pos += 1;
                let inner = self.alternation()?;
                if !self.eat(')') {
                    self.pos = start;
                    return Err(self.error("unclosed group"));
                }
                Ok(inner)
            }
            '[' => self.class(),
            '.' => {
                self.pos += 1;
                Ok(Ast::Any)
            }
            '^' => {
                self.pos += 1;
                Ok(Ast::Start)
            }
            '$' => {
                self.pos += 1;
                Ok(Ast::End)
            }
            '*' | '+' | '?' => Err(self.error("quantifier with nothing to repeat")),
            '\\' => self.escape(),
            _ => {
                self.pos += 1;
                Ok(Ast::Char(c))
            }
        }
    }

    /// The escape face: metacharacters ride through; the shorthands
    /// and the word-boundary family refuse by name (the documented
    /// subset's edge).
    fn escape(&mut self) -> Result<Ast, RegexError> {
        self.pos += 1; // '\\'
        let start = self.pos;
        let Some(c) = self.peek() else {
            return Err(self.error("dangling escape"));
        };
        self.pos += 1;
        match c {
            'd' | 'D' | 'w' | 'W' | 's' | 'S' => Err(RegexError {
                position: start,
                message: format!("the shorthand \\{c} is outside the subset — spell the class"),
            }),
            'b' | 'B' | 'A' | 'Z' | 'z' => Err(RegexError {
                position: start,
                message: format!("the anchor \\{c} is outside the subset — use ^ and $"),
            }),
            _ => Ok(Ast::Char(c)),
        }
    }

    fn class(&mut self) -> Result<Ast, RegexError> {
        let start = self.pos;
        self.pos += 1; // '['
        let negated = self.eat('^');
        let mut ranges = Vec::new();
        let mut first = true;
        loop {
            let Some(c) = self.peek() else {
                self.pos = start;
                return Err(self.error("unclosed class"));
            };
            if c == ']' && !first {
                self.pos += 1;
                return Ok(Ast::Class { ranges, negated });
            }
            first = false;
            let low = if c == '\\' {
                self.pos += 1;
                let Some(escaped) = self.peek() else {
                    self.pos = start;
                    return Err(self.error("dangling escape in class"));
                };
                if matches!(escaped, 'd' | 'D' | 'w' | 'W' | 's' | 'S' | 'b' | 'B') {
                    return Err(RegexError {
                        position: self.pos,
                        message: format!(
                            "the shorthand \\{escaped} is outside the subset — spell the class"
                        ),
                    });
                }
                self.pos += 1;
                escaped
            } else {
                self.pos += 1;
                c
            };
            if self.peek() == Some('-')
                && self.chars.get(self.pos + 1).copied().unwrap_or(']') != ']'
            {
                self.pos += 1; // '-'
                let Some(high) = self.peek() else {
                    self.pos = start;
                    return Err(self.error("unclosed class"));
                };
                self.pos += 1;
                if high == '\\' {
                    let Some(escaped) = self.peek() else {
                        self.pos = start;
                        return Err(self.error("dangling escape in class"));
                    };
                    self.pos += 1;
                    if escaped == ']' || escaped == '-' {
                        // A `-x]` tail reads as the chars `-`, `x`, `]`.
                        ranges.push((low, low));
                        ranges.push(('-', '-'));
                        ranges.push((escaped, escaped));
                        continue;
                    }
                    return Err(RegexError {
                        position: self.pos,
                        message: format!("the shorthand \\{escaped} is outside the subset"),
                    });
                }
                if high < low {
                    return Err(self.error("class range high below low"));
                }
                ranges.push((low, high));
            } else {
                ranges.push((low, low));
            }
        }
    }
}

// ---- the NFA ------------------------------------------------------------

/// One edge's membership test.
#[derive(Clone)]
enum Class {
    /// One exact character.
    Literal(char),
    /// Any character (`.`).
    Any,
    /// A class with its ranges and negation.
    Ranges {
        /// The class's ranges (inclusive).
        ranges: Vec<(char, char)>,
        /// Whether the class negates its ranges.
        negated: bool,
    },
}

impl Class {
    fn matches(&self, c: char) -> bool {
        match self {
            Class::Literal(literal) => *literal == c,
            Class::Any => true,
            Class::Ranges { ranges, negated } => {
                let inside = ranges.iter().any(|(low, high)| *low <= c && c <= *high);
                inside != *negated
            }
        }
    }
}

struct State {
    /// Unconditional epsilon transitions.
    epsilon: Vec<usize>,
    /// Zero-width anchor transitions (taken when the condition holds
    /// at the current input position, without consuming input).
    anchors: Vec<(Anchor, usize)>,
    /// Consuming transitions.
    edges: Vec<(Class, usize)>,
}

/// One zero-width anchor.
#[derive(Clone, Copy)]
enum Anchor {
    /// The `^` anchor — holds at input position 0.
    Start,
    /// The `$` anchor — holds at end-of-input.
    End,
}

/// The compiled machine: a live-state set advancing per input
/// character — linear in input length × pattern size, always.
struct Nfa {
    states: Vec<State>,
    /// The scan state: re-enters the pattern's start at every input
    /// position and consumes anything to stay live — the unanchored
    /// search.
    scan: usize,
    /// The pattern's accepting state.
    accepting: usize,
}

impl Nfa {
    fn build(ast: &Ast) -> Self {
        let mut builder = Builder { states: Vec::new() };
        let (pattern_entry, pattern_exit) = builder.emit(ast);
        builder.states.push(State {
            epsilon: vec![pattern_entry],
            anchors: Vec::new(),
            edges: Vec::new(),
        });
        let scan = builder.states.len();
        builder.states.push(State {
            epsilon: vec![pattern_entry],
            anchors: Vec::new(),
            edges: vec![(Class::Any, scan)],
        });
        Nfa {
            states: builder.states,
            scan,
            accepting: pattern_exit,
        }
    }

    /// Whether the machine matches anywhere in the input.
    fn matches(&self, input: &[char]) -> bool {
        let end = input.len();
        let mut live = vec![false; self.states.len()];
        let mut queue = std::collections::VecDeque::new();
        self.seed_and_close(self.scan, &mut live, &mut queue, 0, end);
        if live[self.accepting] {
            return true;
        }
        for (position, c) in input.iter().enumerate() {
            let mut next = vec![false; self.states.len()];
            let mut next_queue = std::collections::VecDeque::new();
            for (state, is_live) in live.iter().enumerate() {
                if !*is_live {
                    continue;
                }
                for (class, target) in &self.states[state].edges {
                    if class.matches(*c) {
                        self.seed(*target, &mut next, &mut next_queue);
                    }
                }
            }
            // The scan loop consumes any character and stays live —
            // the unanchored search may start over at every position.
            self.seed(self.scan, &mut next, &mut next_queue);
            self.close(&mut next, &mut next_queue, position + 1, end);
            live = next;
            if live[self.accepting] {
                return true;
            }
        }
        false
    }

    fn seed(&self, state: usize, live: &mut [bool], queue: &mut std::collections::VecDeque<usize>) {
        if !live[state] {
            live[state] = true;
            queue.push_back(state);
        }
    }

    /// Seeds from one state and expands epsilon transitions plus the
    /// zero-width anchors whose condition holds at `position` (the
    /// `$` anchor tests against the input's end).
    fn seed_and_close(
        &self,
        state: usize,
        live: &mut [bool],
        queue: &mut std::collections::VecDeque<usize>,
        position: usize,
        end: usize,
    ) {
        self.seed(state, live, queue);
        self.close(live, queue, position, end);
    }

    fn close(
        &self,
        live: &mut [bool],
        queue: &mut std::collections::VecDeque<usize>,
        position: usize,
        end: usize,
    ) {
        while let Some(state) = queue.pop_front() {
            for target in &self.states[state].epsilon {
                self.seed(*target, live, queue);
            }
            for (anchor, target) in &self.states[state].anchors {
                let holds = match anchor {
                    Anchor::Start => position == 0,
                    Anchor::End => position == end,
                };
                if holds {
                    self.seed(*target, live, queue);
                }
            }
        }
    }
}

// ---- the Thompson construction ------------------------------------------

/// Every AST node emits a fragment with one entry and one exit state.
struct Builder {
    states: Vec<State>,
}

impl Builder {
    fn new_state(&mut self) -> usize {
        self.states.push(State {
            epsilon: Vec::new(),
            anchors: Vec::new(),
            edges: Vec::new(),
        });
        self.states.len() - 1
    }

    fn emit(&mut self, ast: &Ast) -> (usize, usize) {
        match ast {
            Ast::Empty => {
                let entry = self.new_state();
                (entry, entry)
            }
            Ast::Char(c) => {
                let entry = self.new_state();
                let exit = self.new_state();
                self.states[entry].edges.push((Class::Literal(*c), exit));
                (entry, exit)
            }
            Ast::Any => {
                let entry = self.new_state();
                let exit = self.new_state();
                self.states[entry].edges.push((Class::Any, exit));
                (entry, exit)
            }
            Ast::Class { ranges, negated } => {
                let entry = self.new_state();
                let exit = self.new_state();
                self.states[entry].edges.push((
                    Class::Ranges {
                        ranges: ranges.clone(),
                        negated: *negated,
                    },
                    exit,
                ));
                (entry, exit)
            }
            Ast::Concat(nodes) => {
                let mut first_entry: Option<usize> = None;
                let mut prev_exit: Option<usize> = None;
                for node in nodes {
                    let (entry, exit) = self.emit(node);
                    if let Some(prev) = prev_exit {
                        self.states[prev].epsilon.push(entry);
                    }
                    first_entry.get_or_insert(entry);
                    prev_exit = Some(exit);
                }
                match (first_entry, prev_exit) {
                    (Some(entry), Some(exit)) => (entry, exit),
                    _ => {
                        let entry = self.new_state();
                        (entry, entry)
                    }
                }
            }
            Ast::Alt(branches) => {
                let entry = self.new_state();
                let exit = self.new_state();
                for branch in branches {
                    let (branch_entry, branch_exit) = self.emit(branch);
                    self.states[entry].epsilon.push(branch_entry);
                    self.states[branch_exit].epsilon.push(exit);
                }
                (entry, exit)
            }
            Ast::Repeat { node, min, max } => {
                let entry = self.new_state();
                let exit = self.new_state();
                // min mandatory copies, concatenated.
                let mut prev_exit = entry;
                for _ in 0..*min {
                    let (node_entry, node_exit) = self.emit(node);
                    self.states[prev_exit].epsilon.push(node_entry);
                    prev_exit = node_exit;
                }
                match max {
                    None => {
                        // The star of one more copy: enter it, loop it
                        // back into itself, and both skip faces.
                        let (node_entry, node_exit) = self.emit(node);
                        self.states[prev_exit].epsilon.push(node_entry);
                        self.states[prev_exit].epsilon.push(exit);
                        self.states[node_exit].epsilon.push(node_entry);
                        self.states[node_exit].epsilon.push(exit);
                    }
                    Some(max) => {
                        // max-min optional copies, each skippable.
                        let mut skips = vec![prev_exit];
                        for _ in 0..max.saturating_sub(*min) {
                            let (node_entry, node_exit) = self.emit(node);
                            self.states[prev_exit].epsilon.push(node_entry);
                            prev_exit = node_exit;
                            skips.push(prev_exit);
                        }
                        for skip in skips {
                            self.states[skip].epsilon.push(exit);
                        }
                    }
                }
                (entry, exit)
            }
            Ast::Start => {
                let entry = self.new_state();
                let exit = self.new_state();
                self.states[entry].anchors.push((Anchor::Start, exit));
                (entry, exit)
            }
            Ast::End => {
                let entry = self.new_state();
                let exit = self.new_state();
                self.states[entry].anchors.push((Anchor::End, exit));
                (entry, exit)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(pattern: &str, text: &str) -> bool {
        let ast = parse(pattern).expect("pattern parses");
        is_match(&ast, text, false)
    }

    fn rejects(pattern: &str) -> String {
        match parse(pattern) {
            Err(error) => error.message,
            Ok(_) => panic!("pattern {pattern:?} should refuse"),
        }
    }

    #[test]
    fn literals_and_any_match_unanchored() {
        assert!(matches("abc", "xxabcyy"));
        assert!(!matches("abc", "ab"));
        assert!(matches("a.c", "abc"));
        assert!(!matches("a.c", "ac"));
        assert!(matches("", "anything"), "the empty pattern matches");
    }

    #[test]
    fn anchors_pin_the_positions() {
        assert!(matches("^abc", "abc"));
        assert!(!matches("^abc", "xabc"));
        assert!(matches("abc$", "xxabc"));
        assert!(!matches("abc$", "abcx"));
        assert!(matches("^abc$", "abc"));
        assert!(!matches("^abc$", "abcx"));
    }

    #[test]
    fn classes_and_quantifiers() {
        assert!(matches("[a-c]+", "bbb"));
        assert!(
            matches("[a-c]+", "bd"),
            "unanchored: the b inside is in [a-c]"
        );
        assert!(!matches("[a-c]+", "xyz"));
        assert!(matches("[^a-c]+", "xyz"));
        assert!(!matches("[^a-c]+", "abc"), "every char is inside [a-c]");
        assert!(matches("a{2,3}", "aaa"));
        assert!(!matches("a{2,3}", "a"));
        assert!(matches("a{2}", "aa"));
        assert!(matches("colou?r", "color"));
        assert!(matches("colou?r", "colour"));
        assert!(matches("a*", ""));
        assert!(matches("a{2,}", "aaaa"));
        // A brace that is not a quantifier reads literally.
        assert!(matches("a{,2}", "a{,2}"));
    }

    #[test]
    fn alternation_and_groups() {
        assert!(matches("cat|dog", "hotdog"));
        assert!(!matches("^cat$|^dog$", "hotdog"));
        assert!(matches("^(cat|dog)$", "dog"));
        assert!(matches("(ab)+", "ababab"));
        assert!(matches("(ab)+", "aba"), "unanchored: the ab prefix matches");
        assert!(!matches("(ab)+", "axb"));
        assert!(matches("((a|b)c)+", "acbc"));
    }

    #[test]
    fn escapes_carry_metacharacters() {
        assert!(matches(r"a\.b", "a.b"));
        assert!(!matches(r"a\.b", "axb"));
        assert!(matches(r"a\*b", "a*b"));
        assert!(matches(r"a\\b", "a\\b"));
        assert!(matches("[\\]]", "]"));
        assert!(matches(r"[\-z]", "-"));
    }

    #[test]
    fn the_shorthands_and_word_bounds_refuse_by_name() {
        assert!(
            rejects(r"\d+").contains("\\d"),
            "the shorthand names itself"
        );
        assert!(rejects(r"\w").contains("\\w"));
        assert!(rejects(r"\bx").contains("\\b"));
        assert!(rejects(r"[\d]").contains("\\d"));
    }

    #[test]
    fn syntax_faults_refuse() {
        assert!(rejects("(ab").contains("unclosed"));
        assert!(rejects("[ab").contains("unclosed"));
        assert!(rejects("*a").contains("nothing to repeat"));
        assert!(rejects("a**").contains("nested"));
        assert!(rejects("a{3,2}").contains("below min"));
        assert!(rejects("^*").contains("anchor"));
    }

    #[test]
    fn iregexp_folds_input_and_pattern() {
        let ast = parse("[A-Z]+ard").expect("parses");
        assert!(is_match(&ast, "HOGWARD", true));
        assert!(is_match(&ast, "hogward", true));
        assert!(!is_match(&ast, "hogward", false));
    }

    #[test]
    fn linear_simulation_handles_the_pathological_shape() {
        // The classic catastrophic-backtracking shape stays linear
        // under the Thompson simulation — a long non-matching input
        // completes (the test's own runtime is the guarantee's pin).
        let ast = parse("(a+)+$").expect("parses");
        let text = "a".repeat(400) + "b";
        assert!(!is_match(&ast, &text, false));
    }
}
