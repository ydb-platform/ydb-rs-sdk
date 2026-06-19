#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ErrorHandlingStrategy {
    FailFast,
    Skip,
}
