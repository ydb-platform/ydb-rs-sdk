#[derive(Clone, PartialEq)]
pub enum ErrorHandlingStrategy {
    FailFast,
    Skip,
}
