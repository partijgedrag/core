use crate::characters::CHARACTERS;
use regex::Regex;

pub fn dutch_month_to_number(month: &str) -> Option<u32> {
    match month.to_lowercase().as_str() {
        "januari" => Some(1),
        "februari" => Some(2),
        "maart" => Some(3),
        "april" => Some(4),
        "mei" => Some(5),
        "juni" => Some(6),
        "juli" => Some(7),
        "augustus" => Some(8),
        "september" => Some(9),
        "oktober" => Some(10),
        "november" => Some(11),
        "december" => Some(12),
        _ => None,
    }
}

pub fn dutch_language_to_language_code(language: &str) -> Option<&str> {
    match language.to_ascii_lowercase().as_str() {
        "nederlands" => Some("NL"),
        "frans" => Some("FR"),
        _ => None,
    }
}

pub fn slugify_name(name: &str) -> String {
    let name = name.to_lowercase();

    // Convert common special characters to ASCII equivalent
    let transliterated = name
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
        .replace("é", "e")
        .replace("è", "e")
        .replace("à", "a")
        .replace("ç", "c")
        .replace("ñ", "n")
        .replace("ø", "o");

    // Remove remaining non-alphanumeric characters except spaces and hyphens
    let re = Regex::new(r"[^a-z0-9\s-]").unwrap();
    let cleaned = re.replace_all(&transliterated, "");
    // Replace spaces with hyphens
    let _ = cleaned.trim().replace(" ", "-");
    cleaned.trim().replace(" ", "-")
}

pub fn clean_text(raw: &str) -> String {
    let cleaned = raw
        .replace(CHARACTERS::NEWLINE, " ")
        .replace(CHARACTERS::SOFT_HYPHEN, "")
        .replace(CHARACTERS::NON_BREAKING_SPACE, " ")
        .trim()
        .to_string();
    cleaned
}
