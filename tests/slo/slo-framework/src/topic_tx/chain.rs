use std::fmt::{Display, Formatter};

use anyhow::{Context, Result, ensure};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(i64);

impl PartitionId {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> i64 {
        self.0
    }
}

impl Display for PartitionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicOffset(i64);

impl TopicOffset {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> i64 {
        self.0
    }
}

impl Display for TopicOffset {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageCoordinate {
    pub partition_id: PartitionId,
    pub offset: TopicOffset,
}

impl Display for MessageCoordinate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "partition {} offset {}",
            self.partition_id, self.offset
        )
    }
}

/// The single live value passed from one transaction to the next in a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainEvent {
    pub partition_id: PartitionId,
    pub generation: u64,
}

impl ChainEvent {
    /// Creates the generation-zero event that starts a partition chain.
    pub fn initial(partition_id: PartitionId) -> Result<Self> {
        ensure!(
            partition_id.value() >= 0,
            "initial chain event has negative partition {partition_id}"
        );

        Ok(Self {
            partition_id,
            generation: 0,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        format!("{}:{}", self.partition_id, self.generation).into_bytes()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut fields = data.split(|byte| *byte == b':');
        let partition_id = PartitionId::new(parse(
            fields.next().context("chain event has no partition")?,
            "partition",
        )?);
        let generation = parse(
            fields.next().context("chain event has no generation")?,
            "generation",
        )?;
        ensure!(
            fields.next().is_none(),
            "chain event contains unexpected fields"
        );
        ensure!(
            partition_id.value() >= 0,
            "chain event has negative partition {partition_id}"
        );

        Ok(Self {
            partition_id,
            generation,
        })
    }
}

/// One validated chain advance and the exact row persisted for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainTransition {
    pub coordinate: MessageCoordinate,
    pub input_generation: u64,
    pub output_generation: u64,
}

impl ChainTransition {
    pub fn new(coordinate: MessageCoordinate, input: ChainEvent) -> Result<Self> {
        ensure!(
            coordinate.offset.value() >= 0,
            "transaction input has negative offset {}",
            coordinate.offset,
        );
        ensure!(
            coordinate.partition_id == input.partition_id,
            "transaction input at {coordinate} contains event for partition {}",
            input.partition_id,
        );
        let expected_generation = u64::try_from(coordinate.offset.value())
            .context("transaction input offset does not fit into generation")?;
        ensure!(
            input.generation == expected_generation,
            "transaction input at {coordinate} has generation {}, expected {expected_generation}",
            input.generation,
        );
        let output_generation = input
            .generation
            .checked_add(1)
            .context("chain event generation overflow")?;

        Ok(Self {
            coordinate,
            input_generation: input.generation,
            output_generation,
        })
    }

    pub fn successor(&self) -> ChainEvent {
        ChainEvent {
            partition_id: self.coordinate.partition_id,
            generation: self.output_generation,
        }
    }
}

fn parse<T>(field: &[u8], name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let text = std::str::from_utf8(field)
        .with_context(|| format!("chain event {name} is not valid UTF-8"))?;
    text.parse()
        .with_context(|| format!("failed to parse chain event {name} from {text:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(partition: i64, generation: u64) -> ChainEvent {
        ChainEvent {
            partition_id: PartitionId::new(partition),
            generation,
        }
    }

    fn coordinate(partition: i64, offset: i64) -> MessageCoordinate {
        MessageCoordinate {
            partition_id: PartitionId::new(partition),
            offset: TopicOffset::new(offset),
        }
    }

    fn transition(partition: i64, offset: i64, generation: u64) -> ChainTransition {
        ChainTransition::new(coordinate(partition, offset), event(partition, generation)).unwrap()
    }

    #[test]
    fn chain_event_round_trip_and_successor_are_deterministic() {
        let event = event(3, 11);
        let transition = transition(3, 11, 11);

        assert_eq!(ChainEvent::decode(&event.encode()).unwrap(), event);
        assert_eq!(transition.successor().generation, 12);
        assert_eq!(transition.successor().partition_id, PartitionId::new(3));
    }

    #[test]
    fn malformed_chain_events_are_rejected() {
        assert!(ChainEvent::decode(b"").is_err());
        assert!(ChainEvent::decode(b"0").is_err());
        assert!(ChainEvent::decode(b"x:1").is_err());
        assert!(ChainEvent::decode(b"-1:1").is_err());
        assert!(ChainEvent::decode(b"0:1:2").is_err());
    }

    #[test]
    fn invalid_chain_transitions_are_rejected() {
        assert!(ChainEvent::initial(PartitionId::new(-1)).is_err());
        assert!(ChainTransition::new(coordinate(0, 0), event(0, u64::MAX)).is_err());
        let negative_offset = ChainTransition::new(coordinate(0, -1), event(0, 0));
        assert!(negative_offset.is_err());

        let wrong_partition = ChainTransition::new(coordinate(0, 0), event(1, 0));
        assert!(wrong_partition.is_err());

        let wrong_generation = ChainTransition::new(coordinate(0, 1), event(0, 0));
        assert!(wrong_generation.is_err());
    }
}
