//! Arrow IPC serialization

use crate::errors::{YdbError, YdbResult};
use arrow_array::RecordBatch;
use arrow_ipc::writer::{DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions};
use arrow_ipc::MetadataVersion;

/// Serialize Arrow RecordBatch to IPC format for bulk upsert
pub(crate) fn serialize_record_batch_for_bulk_upsert(
    batch: &RecordBatch,
) -> YdbResult<(Vec<u8>, Vec<u8>)> {
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .map_err(|e| YdbError::Custom(format!("Failed to create IPC options: {}", e)))?;

    let gen = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);

    let encoded_schema =
        gen.schema_to_bytes_with_dictionary_tracker(&batch.schema(), &mut tracker, &options);

    let (encoded_dictionaries, encoded_batch) = gen
        .encoded_batch(batch, &mut tracker, &options)
        .map_err(|e| YdbError::Custom(format!("Failed to encode batch: {}", e)))?;

    if !encoded_dictionaries.is_empty() {
        return Err(YdbError::Custom(
            "Dictionary encoding not supported".to_string(),
        ));
    }

    Ok((
        to_framed_ipc_message(&encoded_schema),
        to_framed_ipc_message(&encoded_batch),
    ))
}

fn to_framed_ipc_message(encoded: &EncodedData) -> Vec<u8> {
    let metadata_len = encoded.ipc_message.len() as u32;
    let total = 8 + encoded.ipc_message.len() + encoded.arrow_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
    buf.extend_from_slice(&metadata_len.to_le_bytes());
    buf.extend_from_slice(&encoded.ipc_message);
    buf.extend_from_slice(&encoded.arrow_data);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_serialize_simple_batch() -> YdbResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
            ],
        )
        .map_err(|e| YdbError::Custom(format!("Failed to create batch: {}", e)))?;

        let (schema_bytes, data_bytes) = serialize_record_batch_for_bulk_upsert(&batch)?;

        assert!(!schema_bytes.is_empty());
        assert!(!data_bytes.is_empty());

        Ok(())
    }

    #[test]
    fn test_serialize_empty_batch() -> YdbResult<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))])
                .map_err(|e| YdbError::Custom(format!("Failed to create batch: {}", e)))?;

        let (schema_bytes, data_bytes) = serialize_record_batch_for_bulk_upsert(&batch)?;

        assert!(!schema_bytes.is_empty());
        assert!(!data_bytes.is_empty());

        Ok(())
    }
}
