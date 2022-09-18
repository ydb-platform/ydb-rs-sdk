use itertools::Itertools;

const TRACE_VALUE_MAX_LEN: usize = 1000;

pub(crate) fn ensure_len_string(s: String) -> String {
    if s.len() <= TRACE_VALUE_MAX_LEN {
        s
    } else {
        s.chars().take(TRACE_VALUE_MAX_LEN).join("") + "..."
    }
}
