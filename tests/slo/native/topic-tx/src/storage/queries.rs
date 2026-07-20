use anyhow::{Context, Result};
use tokio::time::{Instant, timeout_at};
use ydb::{Row, TopicReader, TopicReaderBatch, Transaction, Value, YdbError, YdbOrCustomerError};

use slo_framework::topic_tx::{
    ChainEvent, ChainTransition, MessageCoordinate, PartitionId, TopicOffset,
};

use super::transaction::invalid_chain_state;

pub(super) async fn read_next_transition(
    reader: &mut TopicReader,
    tx: &mut Transaction,
    expected_partition_id: PartitionId,
    deadline: Instant,
) -> Result<ChainTransition, YdbOrCustomerError> {
    let batch = {
        let mut reader_tx = reader.tx_reader(tx).await?;
        timeout_at(deadline, reader_tx.read_batch())
            .await
            .map_err(|_| YdbError::Custom("transaction topic read timed out".to_string()))??
    };
    transition_from_batch(batch, expected_partition_id).await
}

async fn transition_from_batch(
    batch: TopicReaderBatch,
    expected_partition_id: PartitionId,
) -> Result<ChainTransition, YdbOrCustomerError> {
    let [mut message] = batch.messages.try_into().map_err(|messages: Vec<_>| {
        invalid_chain_state(anyhow::anyhow!(
            "transaction reader returned {} messages instead of one",
            messages.len(),
        ))
    })?;
    let coordinate = MessageCoordinate {
        partition_id: PartitionId::new(message.get_partition_id()),
        offset: TopicOffset::new(message.offset),
    };
    if coordinate.partition_id != expected_partition_id {
        return Err(invalid_chain_state(anyhow::anyhow!(
            "worker for partition {expected_partition_id} received message from partition {}",
            coordinate.partition_id,
        )));
    }
    let data = message.read_and_take().await?.ok_or_else(|| {
        invalid_chain_state(anyhow::anyhow!(
            "topic message at {coordinate} has no payload"
        ))
    })?;
    let event = ChainEvent::decode(&data).map_err(|error| {
        invalid_chain_state(error.context(format!("decode chain event at {coordinate}")))
    })?;
    ChainTransition::new(coordinate, event).map_err(invalid_chain_state)
}

pub(super) async fn upsert_transition(
    tx: &mut Transaction,
    table_path: &str,
    transition: &ChainTransition,
) -> Result<(), YdbOrCustomerError> {
    tx.exec(format!(
        "UPSERT INTO `{table_path}` (
            partition_id, input_offset, input_generation, output_generation
         ) VALUES (
            $partition_id, $input_offset, $input_generation, $output_generation
         )"
    ))
    .param("$partition_id", transition.coordinate.partition_id.value())
    .param("$input_offset", transition.coordinate.offset.value())
    .param("$input_generation", transition.input_generation)
    .param("$output_generation", transition.output_generation)
    .await?;
    Ok(())
}

pub(super) fn transition_from_row(mut row: Row) -> Result<ChainTransition> {
    Ok(ChainTransition {
        coordinate: MessageCoordinate {
            partition_id: PartitionId::new(required_field(&mut row, "partition_id")?),
            offset: TopicOffset::new(required_field(&mut row, "input_offset")?),
        },
        input_generation: required_field(&mut row, "input_generation")?,
        output_generation: required_field(&mut row, "output_generation")?,
    })
}

pub(super) fn required_field<T>(row: &mut Row, name: &str) -> Result<T>
where
    T: TryFrom<Value, Error = YdbError>,
    Option<T>: TryFrom<Value, Error = YdbError>,
{
    let value: Option<T> = row
        .remove_field_by_name(name)
        .with_context(|| format!("read table column {name}"))?
        .try_into()
        .with_context(|| format!("decode table column {name}"))?;
    value.with_context(|| format!("table column {name} is null"))
}
