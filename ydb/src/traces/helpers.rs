use itertools::Itertools;
use std::borrow::Cow;

pub(crate) const TRACE_VALUE_MAX_LEN: usize = 1000;

pub(crate) fn ensure_len_string<'a, S>(text: S) -> Cow<'a, str>
where
    S: Into<Cow<'a, str>>,
{
    let cow = text.into();

    if cow.len() <= TRACE_VALUE_MAX_LEN {
        cow
    } else {
        let mut truncated = cow.chars().take(TRACE_VALUE_MAX_LEN).join("");
        truncated.push_str("...");
        Cow::Owned(truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_len_string_short() {
        let text = "SELECT 1";
        assert_eq!(ensure_len_string(text), text);
    }

    #[test]
    fn test_ensure_len_string_exact() {
        let text: String = "x".repeat(1000);
        assert_eq!(ensure_len_string(&text).len(), 1000);
        assert!(!ensure_len_string(&text).ends_with("..."));
    }

    #[test]
    fn test_ensure_len_string_long() {
        let text: String = "x".repeat(1500);
        let truncated = ensure_len_string(&text);
        assert_eq!(truncated.len(), 1003);
        assert!(truncated.ends_with("..."));
    }
}
