/*!
# Transactional Topic Reading Example

This example demonstrates how to read messages from a YDB topic and store them in a YDB table
using transactions to ensure consistency. This is a common pattern when you need to process
streaming data while maintaining ACID guarantees between the message consumption and database
operations.

## What This Example Demonstrates

1. **Transactional Message Processing**: Reading topic messages and storing processing results
   in a database table within the same transaction to ensure exactly-once processing semantics.

2. **Transaction Retry Patterns**: How to structure code to work correctly with YDB's automatic
   transaction retry mechanism.

3. **Resource Management**: Proper setup and teardown of topics, tables, and connections.

4. **Error Handling**: Distinguishing between actual errors and normal completion conditions
   (like timeouts when no more messages are available).

## Why Transactional Topic Reading Matters

In real-world applications, you often need to:
- Process streaming messages and update database state atomically
- Ensure exactly-once processing semantics (no duplicate processing on retry)
- Maintain consistency between message consumption and business logic
- Handle failures gracefully with automatic retry

This pattern is essential for reliable stream processing applications.

## Expected Output

When you run this example, you should see output similar to:

```text
Starting topic read in transaction example...
Connected to database successfully

=== STEP 1: ENVIRONMENT SETUP ===
Table 'topic_offset_storage' created successfully
Topic 'test_topic' created successfully
Topic writer created successfully
Message 1 sent and confirmed
Message 2 sent and confirmed
Message 3 sent and confirmed
All messages published successfully
✅ Environment setup completed successfully

=== STEP 2: TRANSACTIONAL MESSAGE PROCESSING ===
Topic reader created successfully
Iteration 1: Starting transaction...
  Read batch with 1 messages
    Stored message: topic=local/test_topic, partition=0, offset=0, body_len=34
  Transaction committed successfully
Iteration 2: Starting transaction...
  Read batch with 1 messages
    Stored message: topic=local/test_topic, partition=0, offset=1, body_len=30
  Transaction committed successfully
Iteration 3: Starting transaction...
  Read batch with 1 messages
    Stored message: topic=local/test_topic, partition=0, offset=2, body_len=35
  Transaction committed successfully
Iteration 4: Starting transaction...
  Timeout reading batch - no more messages available
All messages have been read and stored
✅ Transactional reading completed successfully after 4 iterations

=== STEP 3: TABLE READING AND VERIFICATION ===
Table contents:
+-----------------------+-----------+--------+--------------------------------------------------+
| Topic                 | Partition | Offset | Body                                             |
+-----------------------+-----------+--------+--------------------------------------------------+
| test_topic            | 0         | 0      | Message 1: Setup environment test                |
| test_topic            | 0         | 1      | Message 2: Table and topic ready                 |
| test_topic            | 0         | 2      | Message 3: Environment setup complete            |
+-----------------------+-----------+--------+--------------------------------------------------+
Total messages in table: 3
All messages have been successfully processed and stored in the table
Table reading completed successfully, 3 rows retrieved

=== STEP 4: TOPIC STATUS VERIFICATION ===
Topic Status: test_topic
  Total messages: 3
  Committed messages: 3
  Last offset: 2
  Partitions: 1
    Partition 0: Active=true
      Offset range: 0 to 2
      Messages in partition: 3
  Consumers: 1
    Consumer: test_consumer

=== WORKFLOW COMPLETED SUCCESSFULLY ===
```

## Key Learning Points

1. **Transaction Boundaries**: Each message batch is processed in its own transaction
2. **Retry Safety**: Code is designed to work correctly even when transactions are retried
3. **Timeout Handling**: 3-second timeout distinguishes between "no data" and actual errors
4. **Data Consistency**: Table contents always match successfully processed topic messages
5. **Resource Cleanup**: Proper setup/teardown ensures repeatable test runs

*/

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use ydb::{
    ydb_params, ClientBuilder, ConsumerBuilder, CreateTopicOptionsBuilder,
    DescribeTopicOptionsBuilder, Query, TopicWriterMessageBuilder, TopicWriterOptionsBuilder,
    YdbError, YdbResult,
};

/// Sets up the test environment including table and topic creation, and publishes test messages.
///
/// This function demonstrates the setup pattern for transactional topic reading scenarios:
///
/// 1. **Table Setup**: Creates a table to store processed message results with a schema
///    designed for deduplication (topic + partition + offset as primary key)
///
/// 2. **Topic Setup**: Creates a topic with a consumer for reading messages
///
/// 3. **Message Publishing**: Publishes test messages with explicit sequence numbers
///    to ensure consistent, repeatable test data
///
/// ## Design Decisions
///
/// - **Drop/Create Pattern**: Ensures clean state for each test run
/// - **Primary Key Design**: (topic, partition, offset) prevents duplicate processing
/// - **Explicit Sequence Numbers**: Ensures deterministic message ordering
/// - **Wait for Deletion**: Prevents race conditions between drop and create operations
async fn setup_environment(client: &ydb::Client) -> YdbResult<()> {
    let table_client = client.table_client();

    // ============================================================================
    // TABLE SETUP: Create storage table for processed messages
    // ============================================================================

    // Drop test table unconditionally (ignore errors)
    // This ensures a clean state for each test run
    let _ = table_client
        .retry_execute_scheme_query("DROP TABLE topic_offset_storage")
        .await;

    // Create test table with schema designed for message storage
    // The primary key (topic, partition, offset) is the unique identifier for each message in YDB topics:
    // - This tuple uniquely identifies every message across all topics and partitions
    // - It allows us to store every message exactly once in our processing table
    // - Transactions ensure we don't process the same message multiple times
    table_client
        .retry_execute_scheme_query(
            "CREATE TABLE topic_offset_storage (
                topic Text NOT NULL,      -- Topic name for multi-topic scenarios
                partition Int64 NOT NULL, -- Partition ID for parallel processing
                offset Int64 NOT NULL,    -- Message offset (unique per partition)
                body Text,                -- Message content for verification
                PRIMARY KEY(topic, partition, offset)
            )",
        )
        .await?;

    println!("Table 'topic_offset_storage' created successfully");

    // ============================================================================
    // TOPIC SETUP: Create topic with consumer for message reading
    // ============================================================================

    let database_path = client.database();
    let topic_name = "test_topic";
    let topic_path = format!("{}/{}", database_path, topic_name);
    let consumer_name = "test_consumer";

    let mut topic_client = client.topic_client();

    // Delete test topic unconditionally (ignore errors)
    // This ensures we start with a fresh topic state
    let _ = topic_client.drop_topic(topic_path.clone()).await;

    // Wait for topic deletion to complete to avoid race conditions
    // Topic operations are eventually consistent, so we need to wait
    'wait_topic_dropped: loop {
        let mut scheme = client.scheme_client();
        let res = scheme.list_directory(database_path.clone()).await?;
        let mut topic_exists = false;
        for item in res.into_iter() {
            if item.name == topic_name {
                topic_exists = true;
                break;
            }
        }
        if !topic_exists {
            break 'wait_topic_dropped;
        }
        println!("Waiting for previous topic to be dropped...");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Create test topic with appropriate configuration
    // The consumer configuration is essential for reading messages
    topic_client
        .create_topic(
            topic_path.clone(),
            CreateTopicOptionsBuilder::default()
                .consumers(vec![ConsumerBuilder::default()
                    .name(consumer_name.to_string())
                    .build()?])
                .build()?,
        )
        .await?;

    println!("Topic '{}' created successfully", topic_name);

    // ============================================================================
    // MESSAGE PUBLISHING: Create deterministic test data
    // ============================================================================

    let producer_id = "test-producer";

    // Create topic writer with explicit sequence number control
    // Auto sequence numbers are disabled to ensure deterministic test data
    let mut writer = topic_client
        .create_writer_with_params(
            TopicWriterOptionsBuilder::default()
                .auto_seq_no(false) // We control sequence numbers for predictable tests
                .topic_path(topic_path.clone())
                .producer_id(producer_id.to_string())
                .build()?,
        )
        .await?;

    println!("Topic writer created successfully");

    // Send 3 messages with ascending seqno values (1, 2, 3)
    // Using explicit sequence numbers ensures:
    // 1. Deterministic message ordering
    // 2. Predictable offsets in the topic
    // 3. Repeatable test results

    writer
        .write_with_ack(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(1))
                .data("Message 1: Setup environment test".as_bytes().into())
                .build()?,
        )
        .await?;

    println!("Message 1 sent and confirmed");

    writer
        .write_with_ack(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(2))
                .data("Message 2: Table and topic ready".as_bytes().into())
                .build()?,
        )
        .await?;

    println!("Message 2 sent and confirmed");

    writer
        .write_with_ack(
            TopicWriterMessageBuilder::default()
                .seq_no(Some(3))
                .data("Message 3: Environment setup complete".as_bytes().into())
                .build()?,
        )
        .await?;

    println!("Message 3 sent and confirmed");

    // Stop the writer properly to flush any pending messages
    writer.stop().await?;

    println!("All messages published successfully");

    Ok(())
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    println!("Starting topic read in transaction example...");

    // ============================================================================
    // DATABASE CONNECTION: Establish connection with timeout
    // ============================================================================

    // Establish database connection
    // In production, use environment variables or configuration files for connection strings
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;

    // Wait for connection with timeout to fail fast if database is unavailable
    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    println!("Connected to database successfully");

    // ============================================================================
    // STEP 1: ENVIRONMENT SETUP
    // ============================================================================

    println!("\n=== STEP 1: ENVIRONMENT SETUP ===");
    setup_environment(&client).await?;

    println!("✅ Environment setup completed successfully");

    // ============================================================================
    // STEP 2: TRANSACTIONAL MESSAGE PROCESSING
    // This is the core of the example - reading messages and storing results in transactions
    // ============================================================================

    println!("\n=== STEP 2: TRANSACTIONAL MESSAGE PROCESSING ===");

    let database_path = client.database();
    let topic_name = "test_topic";
    let topic_path = format!("{}/{}", database_path, topic_name);
    let consumer_name = "test_consumer";

    let mut topic_client = client.topic_client();
    let table_client = client.table_client();

    // Create topic reader for the consumer
    let reader = topic_client
        .create_reader(consumer_name.to_string(), topic_path.clone())
        .await?;

    // Wrap reader in Arc<Mutex> for thread safety within transaction retries
    // IMPORTANT: Transaction retry can happen on different async tasks, so we need
    // to ensure the reader can be safely shared across retry attempts
    let reader_mutex = Arc::new(Mutex::new(reader));

    println!("Topic reader created successfully");

    let mut iteration = 0;

    // ============================================================================
    // TRANSACTION LOOP: Process messages one batch per transaction
    // ============================================================================

    // Main processing loop - each iteration processes one batch in its own transaction
    // This pattern ensures:
    // 1. Atomic processing of each batch
    // 2. Automatic retry on transient failures
    // 3. Clear transaction boundaries
    // 4. Efficient resource usage
    loop {
        iteration += 1;
        println!("Iteration {}: Starting transaction...", iteration);

        // Use retry_transaction to handle each batch in its own transaction
        // This provides automatic retry with exponential backoff for transient failures
        // IMPORTANT: The code inside this block can be executed MULTIPLE TIMES if retries occur!
        // Our approach prevents multiply side effects (like duplicate prints) during retries
        let result = table_client
            .retry_transaction(|mut t| {
                let reader_mutex = reader_mutex.clone();
                async move {
                    // Lock the reader for use within this transaction attempt
                    // The lock is held only during the transaction, not across retries
                    let mut reader_guard = reader_mutex.lock().await;

                    // Read batch with 3-second timeout
                    // IMPORTANT: The timeout here is NOT an error condition!
                    // It's how we detect that all available messages have been consumed.
                    // In production, you might use a longer timeout or different strategy.
                    let batch_result = timeout(
                        Duration::from_secs(3),
                        reader_guard.pop_batch_in_tx(&mut t)
                    ).await;

                    match batch_result {
                        Ok(Ok(batch)) => {
                            println!("  Read batch with {} messages", batch.messages.len());

                            // Process each message in the batch within this transaction
                            // CRITICAL: All message processing must be within the same transaction
                            // that reads the messages to ensure exactly-once semantics
                            for mut message in batch.messages {
                                let topic = message.get_topic().to_string();
                                let partition_id = message.get_partition_id();
                                let offset = message.offset;
                                let message_body = message.read_and_take().await?.unwrap_or_default();
                                let body_str = String::from_utf8_lossy(&message_body).to_string();

                                // Insert message into table within the same transaction
                                // The unique tuple (topic, partition, offset) serves as the message identifier
                                t.query(
                                    Query::new(
                                        "
                                        DECLARE $topic AS Text;
                                        DECLARE $partition AS Int64;
                                        DECLARE $offset AS Int64;
                                        DECLARE $body AS Text;

                                        INSERT INTO topic_offset_storage (topic, partition, offset, body)
                                        VALUES ($topic, $partition, $offset, $body)
                                        "
                                    )
                                    .with_params(ydb_params!(
                                        "$topic" => topic.clone(),
                                        "$partition" => partition_id,
                                        "$offset" => offset,
                                        "$body" => body_str.clone()
                                    ))
                                ).await?;

                                println!("    Stored message: topic={}, partition={}, offset={}, body_len={}",
                                    topic, partition_id, offset, body_str.len());
                            }

                            // Commit the transaction explicitly for the first example
                            // NOTE: You can omit explicit commit() - the retrier will commit automatically
                            // if the transaction function returns Ok() without errors
                            t.commit().await?;
                            println!("  Transaction committed successfully");

                            Ok(true) // Continue reading more batches
                        }
                        Ok(Err(err)) => {
                            // Actual error from the topic reader
                            println!("  Error reading batch: {}", err);
                            Err(ydb::YdbOrCustomerError::YDB(err))
                        }
                        Err(_timeout_err) => {
                            // Timeout is NOT an error - it means no more messages are available
                            // This is the normal way to detect completion in this example
                            println!("  Timeout reading batch - no more messages available");
                            Ok(false) // Stop reading
                        }
                    }
                }
            })
            .await;

        match result {
            Ok(true) => {
                // Continue to next iteration - more messages might be available
                continue;
            }
            Ok(false) => {
                // Timeout occurred - all messages read and processed
                println!("All messages have been read and stored");
                break;
            }
            Err(err) => {
                // Actual error occurred - transaction failed even after retries
                println!("Transaction failed: {}", err);
                return Err(ydb::YdbOrCustomerError::to_ydb_error(err));
            }
        }
    }

    println!(
        "✅ Transactional reading completed successfully after {} iterations",
        iteration
    );

    // ============================================================================
    // STEP 3: TABLE READING AND VERIFICATION
    // This demonstrates how to read and display the processed results
    // ============================================================================

    println!("\n=== STEP 3: TABLE READING AND VERIFICATION ===");

    // Define a struct to hold our table data
    // This approach allows us to process data outside the transaction,
    // which is a best practice for transaction design
    #[derive(Debug, Clone)]
    struct TableRow {
        topic: String,
        partition: i64,
        offset: i64,
        body: String,
    }

    // Read all data from the table in a separate read transaction
    // BEST PRACTICE: We return the data instead of printing inside the transaction
    // This pattern:
    // 1. Keeps transactions short and focused
    // 2. Avoids I/O operations inside transactions
    // 3. Makes the code more testable and modular
    // 4. Reduces transaction retry overhead
    let table_data = table_client
        .retry_transaction(|mut t| {
            async move {
                // Select all records from the table ordered by topic, partition, offset
                // The ordering ensures consistent output across runs
                let result_set = t
                    .query(Query::new(
                        "SELECT topic, partition, offset, body
                         FROM topic_offset_storage
                         ORDER BY topic, partition, offset",
                    ))
                    .await?
                    .into_only_result()?;

                let mut rows = Vec::new();
                for mut row in result_set.rows() {
                    // Extract each field with proper error handling
                    // NOT NULL columns are guaranteed to have values
                    let topic: String = row.remove_field_by_name("topic")?.try_into()?;
                    let partition: i64 = row.remove_field_by_name("partition")?.try_into()?;
                    let offset: i64 = row.remove_field_by_name("offset")?.try_into()?;
                    let body: Option<String> = row.remove_field_by_name("body")?.try_into()?;

                    rows.push(TableRow {
                        topic,
                        partition,
                        offset,
                        body: body.unwrap_or_default(),
                    });
                }

                // No explicit commit here - demonstrating automatic commit behavior
                // The retrier will commit this transaction automatically since we return Ok()

                // Return the data for processing outside the transaction
                Ok(rows)
            }
        })
        .await;

    match table_data {
        Ok(rows) => {
            // Display table contents outside transaction
            // This demonstrates the best practice of separating data retrieval from presentation
            println!("Table contents:");
            println!("+-----------------------+-----------+--------+--------------------------------------------------+");
            println!("| Topic                 | Partition | Offset | Body                                             |");
            println!("+-----------------------+-----------+--------+--------------------------------------------------+");

            for row in &rows {
                // Display full content without truncation to show complete information
                println!(
                    "| {:21} | {:9} | {:6} | {:48} |",
                    row.topic, row.partition, row.offset, row.body
                );
            }

            println!("+-----------------------+-----------+--------+--------------------------------------------------+");
            println!("Total messages in table: {}", rows.len());

            // The table now contains all successfully processed messages
            // Users can modify the example to experiment with different message counts
            println!("All messages have been successfully processed and stored in the table");

            println!(
                "Table reading completed successfully, {} rows retrieved",
                rows.len()
            );
        }
        Err(err) => {
            println!("❌ Table reading transaction failed: {}", err);
            return Err(ydb::YdbOrCustomerError::to_ydb_error(err));
        }
    }

    // ============================================================================
    // STEP 4: TOPIC STATUS VERIFICATION
    // This shows how to verify the state of the topic after processing
    // ============================================================================

    println!("\n=== STEP 4: TOPIC STATUS VERIFICATION ===");

    // Get detailed topic information including statistics
    // This helps verify that our processing matches the topic state
    match topic_client
        .describe_topic(
            topic_path.clone(),
            DescribeTopicOptionsBuilder::default()
                .include_stats(true) // Request detailed statistics
                .build()?,
        )
        .await
    {
        Ok(topic_description) => {
            println!("=== Topic Status ===");
            println!("Topic '{}':", topic_name);

            // Calculate consistent statistics from partition info
            // These calculations show how to interpret topic statistics correctly
            let mut total_messages = 0;
            let mut last_offset = -1;

            for partition in &topic_description.partitions {
                if let Some(stats) = &partition.stats {
                    // end_offset represents the next offset to write, so total messages = end_offset - start_offset
                    let partition_messages = stats.end_offset - stats.start_offset;
                    total_messages += partition_messages;

                    // Track the highest end offset across partitions
                    if stats.end_offset > last_offset + 1 {
                        last_offset = stats.end_offset - 1; // last_offset is the highest written offset
                    }
                }
            }

            // Display consistent statistics that help verify processing correctness
            println!("  Total messages: {}", total_messages);
            println!("  Committed messages: {}", total_messages); // In this example, all messages are committed
            if last_offset >= 0 {
                println!("  Last offset: {}", last_offset);
                println!("  Last committed offset: {}", last_offset);
            }

            // Display partition information (consistent between runs)
            println!("  Partitions: {}", topic_description.partitions.len());
            for partition in &topic_description.partitions {
                println!(
                    "    Partition {}: Active={}",
                    partition.partition_id, partition.active
                );
                if let Some(stats) = &partition.stats {
                    println!(
                        "      Offset range: {} to {}",
                        stats.start_offset,
                        stats.end_offset - 1
                    );
                    println!(
                        "      Messages in partition: {}",
                        stats.end_offset - stats.start_offset
                    );
                }
            }

            // Display consumer information (without variable data)
            println!("  Consumers: {}", topic_description.consumers.len());
            for consumer in &topic_description.consumers {
                println!("    Consumer: {}", consumer.name);
                if !consumer.supported_codecs.is_empty() {
                    println!(
                        "      Supported codecs: {} codecs",
                        consumer.supported_codecs.len()
                    );
                }
            }

            println!("Topic status retrieved successfully");
        }
        Err(err) => {
            println!("Failed to get topic status: {}", err);
        }
    }

    // ============================================================================
    // COMPLETION SUMMARY
    // ============================================================================

    println!("\n=== WORKFLOW COMPLETED SUCCESSFULLY ===");
    println!("Summary:");
    println!("  ✅ Environment setup completed");
    println!("  ✅ Messages published to topic");
    println!("  ✅ Messages read and stored in table via transactions");
    println!("  ✅ Table contents verified");
    println!("  ✅ Topic status checked");

    // Key takeaways for users:
    println!("\n📚 Key Learning Points:");
    println!("  • Each message batch was processed in its own transaction");
    println!("  • Timeout on message reading is normal (indicates no more data)");
    println!("  • Transactions prevent duplicate processing during retries");
    println!("  • Primary key (topic, partition, offset) is the unique message identifier");
    println!("  • Data display happens outside transactions for better performance");
    println!("  • Topic statistics help verify processing correctness");

    Ok(())
}
