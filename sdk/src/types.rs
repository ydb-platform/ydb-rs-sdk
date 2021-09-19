enum YdbValue {
    NULL,
    BOOL(bool),
    INT32(i32),
    UINT32(u32),
    INT64(i64),
    UINT64(u64),
    INT128(i128),
    FLOAT32(f32),
    FLOAT64(f64),
    BYTES(Vec<u8>),
    TEXT(String),
}
