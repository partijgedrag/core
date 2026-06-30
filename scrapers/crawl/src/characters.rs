pub struct CHARACTERS;

impl CHARACTERS {
    pub const NEWLINE: &'static str = "\n";
    pub const SOFT_HYPHEN: char = '\u{00AD}';  // Unicode for &shy;
    pub const NON_BREAKING_SPACE: char = '\u{00A0}'; // Unicode for non-breaking space
}