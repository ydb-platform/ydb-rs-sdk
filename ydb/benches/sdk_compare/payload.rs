use anyhow::{Result, ensure};

pub(crate) const HEADER_SIZE_BYTES: usize = 8;

const FILL_BYTE: u8 = 0xA5;

pub(crate) fn allocate(message_size_bytes: usize) -> Result<Vec<u8>> {
    ensure!(
        message_size_bytes >= HEADER_SIZE_BYTES,
        "message size must be at least {HEADER_SIZE_BYTES} bytes"
    );

    let mut payload = vec![FILL_BYTE; message_size_bytes];
    payload[..HEADER_SIZE_BYTES].fill(0);
    Ok(payload)
}

pub(crate) fn write_timestamp(payload: &mut [u8], sent_at_ns: u64) -> Result<()> {
    ensure!(
        payload.len() >= HEADER_SIZE_BYTES,
        "payload is shorter than the {HEADER_SIZE_BYTES}-byte header"
    );
    payload[..HEADER_SIZE_BYTES].copy_from_slice(&sent_at_ns.to_le_bytes());
    Ok(())
}

pub(crate) fn read_timestamp(payload: &[u8]) -> Result<u64> {
    ensure!(
        payload.len() >= HEADER_SIZE_BYTES,
        "payload is shorter than the {HEADER_SIZE_BYTES}-byte header"
    );

    let mut sent_at_ns = [0_u8; 8];
    sent_at_ns.copy_from_slice(&payload[..HEADER_SIZE_BYTES]);
    Ok(u64::from_le_bytes(sent_at_ns))
}
