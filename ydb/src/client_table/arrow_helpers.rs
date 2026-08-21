//! Arrow IPC serialization

use crate::errors::{YdbError, YdbResult};
use arrow_array::RecordBatch;
use arrow_ipc::MetadataVersion;
use arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions, write_message,
};

/// Serialize Arrow RecordBatch to IPC format for bulk upsert
pub(super) async fn serialize_record_batch_for_bulk_upsert(
    batch: RecordBatch,
) -> YdbResult<(Vec<u8>, Vec<u8>)> {
    tokio::task::spawn_blocking(move || serialize_record_batch(&batch)).await?
}

fn serialize_record_batch(batch: &RecordBatch) -> YdbResult<(Vec<u8>, Vec<u8>)> {
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .map_err(|e| YdbError::Custom(format!("Failed to create IPC options: {}", e)))?;

    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);

    let encoded_schema =
        generator.schema_to_bytes_with_dictionary_tracker(&batch.schema(), &mut tracker, &options);

    let (encoded_dictionaries, encoded_batch) = generator
        .encoded_batch(batch, &mut tracker, &options)
        .map_err(|e| YdbError::Custom(format!("Failed to encode batch: {}", e)))?;

    if !encoded_dictionaries.is_empty() {
        return Err(YdbError::Custom(
            "Dictionary encoding not supported".to_string(),
        ));
    }

    Ok((
        frame_ipc_message(encoded_schema, &options)?,
        frame_ipc_message(encoded_batch, &options)?,
    ))
}

fn frame_ipc_message(encoded: EncodedData, options: &IpcWriteOptions) -> YdbResult<Vec<u8>> {
    let mut buf = Vec::new();
    write_message(&mut buf, encoded, options)
        .map_err(|e| YdbError::Custom(format!("Failed to frame IPC message: {e}")))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_array::types::Int8Type;
    use arrow_array::{Int64Array, StringArray};
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Cursor;
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

        let (schema_bytes, data_bytes) = serialize_record_batch(&batch)?;

        assert!(!schema_bytes.is_empty());
        assert!(!data_bytes.is_empty());

        let mut stream_bytes = schema_bytes;
        stream_bytes.extend_from_slice(&data_bytes);
        let mut reader = StreamReader::try_new(Cursor::new(stream_bytes), None)
            .map_err(|e| YdbError::Custom(format!("Failed to read IPC schema: {e}")))?;
        let decoded = reader
            .next()
            .transpose()
            .map_err(|e| YdbError::Custom(format!("Failed to read IPC batch: {e}")))?
            .ok_or_else(|| YdbError::Custom("IPC stream did not contain a batch".to_string()))?;

        assert_eq!(decoded, batch);

        Ok(())
    }

    #[test]
    fn test_serialize_empty_batch() -> YdbResult<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))])
                .map_err(|e| YdbError::Custom(format!("Failed to create batch: {}", e)))?;

        let (schema_bytes, data_bytes) = serialize_record_batch(&batch)?;

        assert!(!schema_bytes.is_empty());
        assert!(!data_bytes.is_empty());

        Ok(())
    }

    #[test]
    fn test_serialize_dictionary_batch_is_rejected() -> YdbResult<()> {
        let mut values = StringDictionaryBuilder::<Int8Type>::new();
        values.append("Alice").unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "name",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            )])),
            vec![Arc::new(values.finish())],
        )
        .map_err(|e| YdbError::Custom(format!("Failed to create batch: {e}")))?;

        assert!(matches!(
            serialize_record_batch(&batch),
            Err(YdbError::Custom(message)) if message == "Dictionary encoding not supported"
        ));
        Ok(())
    }
}
