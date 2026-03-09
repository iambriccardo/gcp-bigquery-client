//! BigQuery Storage Write API client for high-throughput data streaming.
//!
//! This module provides an implementation of the BigQuery Storage Write API,
//! enabling efficient streaming of structured data to BigQuery tables.

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, Stream};
use futures::StreamExt;
use pin_project::pin_project;
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto,
};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{
    collections::HashMap,
    convert::TryInto,
    fmt::Display,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tonic::{
    codec::CompressionEncoding,
    transport::{Channel, ClientTlsConfig},
    Code, Request, Status, Streaming,
};
use tracing::{debug, warn};

use crate::google::cloud::bigquery::storage::v1::{GetWriteStreamRequest, ProtoRows, WriteStream, WriteStreamView};
use crate::{
    auth::Authenticator,
    error::BQError,
    google::cloud::bigquery::storage::v1::{
        append_rows_request::{self, MissingValueInterpretation, ProtoData},
        append_rows_response,
        big_query_write_client::BigQueryWriteClient,
        AppendRowsRequest, AppendRowsResponse, ProtoSchema,
    },
    BIG_QUERY_V2_URL,
};

/// Base URL for the BigQuery Storage Write API endpoint.
static BIG_QUERY_STORAGE_API_URL: &str = "https://bigquerystorage.googleapis.com";
/// Domain name for BigQuery Storage API used in TLS configuration.
static BIGQUERY_STORAGE_API_DOMAIN: &str = "bigquerystorage.googleapis.com";
/// Maximum size limit for batched append requests in bytes.
///
/// Set to 9MB to provide safety margin under the 10MB BigQuery API limit,
/// accounting for request metadata overhead.
pub const MAX_BATCH_SIZE_BYTES: usize = 9 * 1024 * 1024;
/// Maximum message size for tonic gRPC client configuration.
///
/// Set to 20MB to accommodate large response messages and provide headroom
/// for metadata while staying within reasonable memory bounds.
const MAX_MESSAGE_SIZE_BYTES: usize = 20 * 1024 * 1024;
/// The name of the default stream in BigQuery.
///
/// This stream is a special built-in stream that always exists for a table.
const DEFAULT_STREAM_NAME: &str = "_default";
/// Default number of connections in the pool.
///
/// With HTTP/2 multiplexing, a small number of connections (2-4) is sufficient
/// for high throughput since multiple gRPC streams share each connection.
/// Google recommends reusing connections extensively—a single connection can
/// support 1-10+ MBps throughput. Multiple connections provide fault isolation
/// rather than increased parallelism.
const DEFAULT_CONNECTION_POOL_SIZE: usize = 4;
/// Per-worker channel capacity for handing off jobs to connection workers.
///
/// This is intentionally small because the global semaphore is the primary
/// backpressure mechanism. The worker channel is only a bounded handoff queue.
const CONNECTION_WORKER_CHANNEL_CAPACITY: usize = 4;

/// Default maximum inflight requests per connection.
///
/// HTTP/2 connections can handle many concurrent streams efficiently. Google
/// recommends up to 100 concurrent streams per connection for optimal throughput.
const DEFAULT_REQUESTS_PER_CONNECTION: usize = 100;
/// HTTP/2 keepalive interval in seconds.
///
/// Sends PING frames at this interval to keep connections alive and detect
/// dead connections. Set to 30 seconds to stay well under typical proxy
/// idle timeouts (GCP: 10 min, AWS ELB: 60 sec).
const HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 30;
/// HTTP/2 keepalive timeout in seconds.
///
/// If a PING is not acknowledged within this time, the connection is
/// considered dead.
const HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 10;
/// Maximum number of attempts for retryable append failures.
const MAX_APPEND_RETRY_ATTEMPTS: u32 = 10;
/// Initial retry backoff in milliseconds for retryable append failures.
const INITIAL_APPEND_RETRY_BACKOFF_MS: u64 = 500;
/// Maximum retry backoff in milliseconds for retryable append failures.
const MAX_APPEND_RETRY_BACKOFF_MS: u64 = 60_000;

/// Returns true when a request-level status code should be retried.
fn is_retryable_append_status(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable
        | Code::Internal
        | Code::Aborted
        | Code::Cancelled
        | Code::DeadlineExceeded
        | Code::ResourceExhausted => true,
        Code::Unknown => {
            let message = status.message().to_lowercase();
            message.contains("transport") || message.contains("connection")
        }
        _ => is_idle_stream_close_message(status.message()),
    }
}

/// Returns true for the known BigQuery idle stream close message.
///
/// This was taken from the Java BigQuery SDK.
fn is_idle_stream_close_message(message: &str) -> bool {
    message.contains("Closing the stream because it has been inactive")
}

/// Converts a [`google.rpc.Status`] from in-stream append responses into tonic [`Status`].
fn status_from_rpc_status(status: &crate::google::rpc::Status) -> Status {
    let code = Code::from_i32(status.code);
    Status::new(code, status.message.clone())
}

/// Normalizes a single append response message to success or request-level error.
///
/// The Storage Write API models per-request failures in the response `oneof` as `error`.
/// Treating this as an explicit error keeps caller behavior consistent with transport errors.
fn normalize_append_response(
    response: AppendRowsResponse,
    batch_index: usize,
    stream_name: &str,
) -> Result<AppendRowsResponse, Status> {
    match response.response.as_ref() {
        Some(append_rows_response::Response::AppendResult(_)) => Ok(response),
        Some(append_rows_response::Response::Error(status)) => Err(status_from_rpc_status(status)),
        None => {
            warn!(
                batch_index,
                stream_name = %stream_name,
                "append response missing append_result/error outcome"
            );

            Err(Status::unavailable(
                "append response missing append_result/error outcome",
            ))
        }
    }
}

/// Returns true when a batch should be retried based on response outcomes.
///
/// Retries occur only when all responses are retryable request-level errors.
/// This avoids replaying already-acknowledged successful appends in mixed
/// partial-success/partial-failure attempts.
///
/// Row-level errors are considered permanent and are not retried.
fn should_retry_batch_responses(batch_responses: &[Result<AppendRowsResponse, Status>]) -> bool {
    for response in batch_responses {
        match response {
            Ok(_) => return false,
            Err(status) => {
                if !is_retryable_append_status(status) {
                    return false;
                }
            }
        }
    }

    true
}

/// Calculates exponential backoff for append retries.
fn calculate_append_retry_backoff(attempt: u32) -> Duration {
    let exponential = INITIAL_APPEND_RETRY_BACKOFF_MS
        .saturating_mul(1u64 << attempt.min(10))
        .min(MAX_APPEND_RETRY_BACKOFF_MS);

    Duration::from_millis(exponential)
}

/// Ensures a batch has at least one append response.
///
/// A successfully started append RPC that yields no stream messages is an anomalous condition
/// and must be treated as a request failure rather than a successful append.
fn ensure_non_empty_batch_responses(
    batch_responses: &mut Vec<Result<AppendRowsResponse, Status>>,
    batch_index: usize,
    stream_name: &str,
) {
    if batch_responses.is_empty() {
        warn!(
            batch_index,
            stream_name = %stream_name,
            "append response stream ended without responses"
        );

        batch_responses.push(Err(Status::unavailable(
            "append response stream ended without responses",
        )));
    }
}

/// Configuration for the BigQuery Storage Write API client.
///
/// A single HTTP/2 connection can support 1-10+ MBps throughput with up to
/// 100 concurrent gRPC streams. Multiple workers provide fault isolation and
/// parallel request handling across independent connections.
#[derive(Debug, Clone)]
pub struct StorageApiConfig {
    /// Number of connection workers.
    ///
    /// This controls how many background worker tasks are spawned, each owning
    /// one gRPC connection. The field name is kept for backward compatibility.
    /// Default: 4.
    pub connection_pool_size: usize,
    /// Maximum number of inflight gRPC requests across all StorageApi operations.
    ///
    /// This is a global limit shared across all concurrent calls to the StorageApi.
    /// Each request acquires a permit from a shared semaphore before making a gRPC
    /// call. Google recommends up to 100 concurrent streams per connection.
    /// Default: 400 (connection_pool_size × 100 requests per connection).
    pub max_inflight_requests: usize,
}

impl StorageApiConfig {
    /// Creates a new configuration with the specified connection pool size.
    pub fn with_connection_pool_size(mut self, connection_pool_size: usize) -> Self {
        self.connection_pool_size = connection_pool_size;
        self
    }

    /// Creates a new configuration with the specified max inflight requests.
    pub fn with_max_inflight_requests(mut self, max_inflight_requests: usize) -> Self {
        self.max_inflight_requests = max_inflight_requests;
        self
    }
}

impl Default for StorageApiConfig {
    fn default() -> Self {
        Self {
            connection_pool_size: DEFAULT_CONNECTION_POOL_SIZE,
            max_inflight_requests: DEFAULT_CONNECTION_POOL_SIZE * DEFAULT_REQUESTS_PER_CONNECTION,
        }
    }
}

type WorkerAppendResult = Vec<Result<AppendRowsResponse, Status>>;

/// Supported protobuf column types for BigQuery schema mapping.
#[derive(Debug, Copy, Clone)]
pub enum ColumnType {
    Double,
    Float,
    Int64,
    Uint64,
    Int32,
    Fixed64,
    Fixed32,
    Bool,
    String,
    Bytes,
    Uint32,
    Sfixed32,
    Sfixed64,
    Sint32,
    Sint64,
}

impl From<ColumnType> for Type {
    /// Converts [`ColumnType`] to protobuf [`Type`] enum value.
    ///
    /// Maps each column type variant to its corresponding protobuf type
    /// identifier used in BigQuery Storage Write API schema definitions.
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Double => Type::Double,
            ColumnType::Float => Type::Float,
            ColumnType::Int64 => Type::Int64,
            ColumnType::Uint64 => Type::Uint64,
            ColumnType::Int32 => Type::Int32,
            ColumnType::Fixed64 => Type::Fixed64,
            ColumnType::Fixed32 => Type::Fixed32,
            ColumnType::Bool => Type::Bool,
            ColumnType::String => Type::String,
            ColumnType::Bytes => Type::Bytes,
            ColumnType::Uint32 => Type::Uint32,
            ColumnType::Sfixed32 => Type::Sfixed32,
            ColumnType::Sfixed64 => Type::Sfixed64,
            ColumnType::Sint32 => Type::Sint32,
            ColumnType::Sint64 => Type::Sint64,
        }
    }
}

/// Field cardinality modes for BigQuery schema fields.
#[derive(Debug, Copy, Clone)]
pub enum ColumnMode {
    /// Field may contain null values.
    Nullable,
    /// Field must always contain a value.
    Required,
    /// Field contains an array of values.
    Repeated,
}

impl From<ColumnMode> for Label {
    /// Converts [`ColumnMode`] to protobuf [`Label`] enum value.
    ///
    /// Maps field cardinality modes to their corresponding protobuf labels
    /// used in BigQuery Storage Write API schema definitions.
    fn from(value: ColumnMode) -> Self {
        match value {
            ColumnMode::Nullable => Label::Optional,
            ColumnMode::Required => Label::Required,
            ColumnMode::Repeated => Label::Repeated,
        }
    }
}

/// Metadata descriptor for a single field in a BigQuery table schema.
///
/// Contains the complete field definition including data type, cardinality mode,
/// and protobuf field number required for BigQuery Storage Write API operations.
/// Each field descriptor maps directly to a protobuf field descriptor in the
/// generated schema.
#[derive(Debug, Clone)]
pub struct FieldDescriptor {
    /// Unique field number starting from 1, incrementing for each field.
    pub number: u32,
    /// Name of the field as it appears in BigQuery.
    pub name: String,
    /// Data type of the field.
    pub typ: ColumnType,
    /// Cardinality mode of the field.
    pub mode: ColumnMode,
}

/// Complete schema definition for a BigQuery table.
///
/// Aggregates all field descriptors that define the table's structure.
/// Used to generate protobuf schemas for BigQuery Storage Write API operations
/// and validate row data before transmission.
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    /// Collection of field descriptors defining the table schema.
    pub field_descriptors: Vec<FieldDescriptor>,
}

/// Internal storage for [`TableBatch`] data.
#[derive(Debug)]
struct TableBatchInner<M> {
    stream_name: StreamName,
    table_descriptor: TableDescriptor,
    rows: Vec<M>,
}

/// Collection of rows targeting a specific BigQuery table for batch processing.
///
/// Encapsulates rows with their destination stream and schema metadata,
/// enabling efficient batch operations and optimal parallelism distribution
/// across multiple tables in concurrent append operations. Cloning is cheap
/// as the data is stored behind an [`Arc`].
#[derive(Debug)]
pub struct TableBatch<M>(Arc<TableBatchInner<M>>);

impl<M> Clone for TableBatch<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M> TableBatch<M> {
    /// Creates a new table batch targeting the specified stream.
    ///
    /// Combines rows with their destination metadata to form a complete
    /// batch ready for processing by append operations.
    pub fn new(stream_name: StreamName, table_descriptor: TableDescriptor, rows: Vec<M>) -> Self {
        Self(Arc::new(TableBatchInner {
            stream_name,
            table_descriptor,
            rows,
        }))
    }

    /// Returns the target stream name.
    pub fn stream_name(&self) -> &StreamName {
        &self.0.stream_name
    }

    /// Returns the table descriptor.
    pub fn table_descriptor(&self) -> &TableDescriptor {
        &self.0.table_descriptor
    }

    /// Returns the rows in this batch.
    pub fn rows(&self) -> &[M] {
        &self.0.rows
    }
}

/// A table batch paired with the trace identifier to use for its append requests.
#[derive(Debug)]
pub struct BatchAppendRequest<M> {
    /// The batch to append.
    table_batch: TableBatch<M>,
    /// The trace identifier propagated to BigQuery for this logical batch.
    trace_id: String,
}

impl<M> BatchAppendRequest<M> {
    /// Creates a new batch append request.
    pub fn new(table_batch: TableBatch<M>, trace_id: String) -> Self {
        Self { table_batch, trace_id }
    }
}

/// Result of processing a single table batch in concurrent append operations.
///
/// Contains the batch processing results along with metadata about the operation,
/// including the original batch index for result ordering and total bytes sent
/// for monitoring and debugging purposes.
#[derive(Debug)]
pub struct BatchAppendResult {
    /// Original index of the batch in the input vector.
    ///
    /// Allows callers to correlate results with their original batch ordering
    /// even when results are returned in completion order rather than submission order.
    pub batch_index: usize,
    /// Collection of append operation responses for this batch.
    ///
    /// Each batch may generate multiple append requests due to size limits,
    /// resulting in multiple responses. All responses must be checked for
    /// errors to ensure complete batch success.
    pub responses: Vec<Result<AppendRowsResponse, Status>>,
    /// Total bytes sent for this batch across all requests.
    pub bytes_sent: usize,
}

impl BatchAppendResult {
    /// Creates a new batch append result.
    ///
    /// Combines all result metadata into a single cohesive structure
    /// for easier handling by calling code.
    pub fn new(batch_index: usize, responses: Vec<Result<AppendRowsResponse, Status>>, bytes_sent: usize) -> Self {
        Self {
            batch_index,
            responses,
            bytes_sent,
        }
    }

    /// Returns true if all responses in this batch are successful append results.
    ///
    /// A successful response must satisfy all of:
    /// - transport succeeded (`Ok`),
    /// - response has no row-level errors,
    /// - response outcome is `append_result` (not missing and not `error`).
    pub fn is_success(&self) -> bool {
        self.responses.iter().all(|result| {
            let response = match result {
                Ok(response) => response,
                Err(_) => return false,
            };

            response.row_errors.is_empty()
                && matches!(
                    response.response.as_ref(),
                    Some(append_rows_response::Response::AppendResult(_))
                )
        })
    }
}

/// Hierarchical identifier for BigQuery write streams.
///
/// Represents the complete resource path structure used by BigQuery to
/// uniquely identify tables and their associated write streams within
/// the Google Cloud resource hierarchy.
#[derive(Debug, Clone)]
pub struct StreamName {
    /// Google Cloud project identifier.
    project: String,
    /// BigQuery dataset identifier within the project.
    dataset: String,
    /// BigQuery table identifier within the dataset.
    table: String,
    /// Write stream identifier for the table.
    stream: String,
}

impl StreamName {
    /// Creates a stream name with all components specified.
    ///
    /// Constructs a fully qualified stream identifier using custom
    /// project, dataset, table, and stream components.
    pub fn new(project: String, dataset: String, table: String, stream: String) -> StreamName {
        StreamName {
            project,
            dataset,
            table,
            stream,
        }
    }

    /// Creates a stream name using the default stream identifier.
    ///
    /// Uses "_default" as the stream component, which is the standard
    /// stream identifier for most BigQuery write operations.
    pub fn new_default(project: String, dataset: String, table: String) -> StreamName {
        StreamName {
            project,
            dataset,
            table,
            stream: DEFAULT_STREAM_NAME.to_string(),
        }
    }

    /// Returns the BigQuery table identifier.
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Display for StreamName {
    /// Formats the stream name as a BigQuery resource path.
    ///
    /// Produces the fully qualified resource identifier expected by
    /// BigQuery Storage Write API in the format:
    /// `projects/{project}/datasets/{dataset}/tables/{table}/streams/{stream}`
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let StreamName {
            project,
            dataset,
            table,
            stream,
        } = self;
        f.write_fmt(format_args!(
            "projects/{project}/datasets/{dataset}/tables/{table}/streams/{stream}"
        ))
    }
}

/// Streaming adapter that converts message batches into [`AppendRowsRequest`] objects.
///
/// Automatically chunks large batches into multiple requests while respecting
/// the 10MB BigQuery API size limit. If a single row exceeds the configured
/// limit, it is sent alone and may be rejected by the server. Implements [`Stream`] for seamless
/// integration with async streaming workflows and gRPC client operations.
#[pin_project]
#[derive(Debug)]
pub struct AppendRequestsStream<M> {
    /// Table batch containing rows and metadata for append requests.
    #[pin]
    table_batch: TableBatch<M>,
    /// Protobuf schema definition for the target table.
    proto_schema: ProtoSchema,
    /// Current position in the batch being processed.
    current_index: usize,
    /// Whether to include writer schema in the next request (first only).
    ///
    /// This boolean is used under the assumption that a batch of append requests belongs to the same
    /// table and has no schema differences between the rows.
    include_schema_next: bool,
    /// Shared atomic counter for tracking total bytes sent across all requests in this stream.
    bytes_sent_counter: Arc<AtomicUsize>,
    /// Trace identifier propagated to all append requests in this batch.
    trace_id: String,
}

impl<M> AppendRequestsStream<M> {
    /// Creates a new streaming adapter from a table batch.
    ///
    /// Initializes the stream with all necessary metadata for generating
    /// properly formatted append requests. The schema is included only
    /// in the first request of the stream.
    fn new(
        table_batch: TableBatch<M>,
        proto_schema: ProtoSchema,
        bytes_sent_counter: Arc<AtomicUsize>,
        trace_id: String,
    ) -> Self {
        Self {
            table_batch,
            proto_schema,
            current_index: 0,
            include_schema_next: true,
            bytes_sent_counter,
            trace_id,
        }
    }
}

impl<M> Stream for AppendRequestsStream<M>
where
    M: Message,
{
    type Item = AppendRowsRequest;

    /// Produces the next append request from the message batch.
    ///
    /// Processes messages sequentially, accumulating them into requests
    /// until the size limit is reached. Returns [`Poll::Ready(None)`]
    /// when all messages have been consumed. Each request contains the
    /// maximum number of messages that fit within size constraints.
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let rows = this.table_batch.rows();

        if *this.current_index >= rows.len() {
            return Poll::Ready(None);
        }

        let mut serialized_rows = Vec::new();
        let mut total_size = 0;
        let mut processed_count = 0;

        // Process messages from `current_index` onwards. We do not change the vector while processing
        // to avoid reallocations which are unnecessary.
        for msg in rows.iter().skip(*this.current_index) {
            // First, check the encoded length to avoid performing a full encode
            // on the first message that would exceed the limit and be dropped.
            let size = msg.encoded_len();
            if total_size + size > MAX_BATCH_SIZE_BYTES && !serialized_rows.is_empty() {
                break;
            }

            // Safe to encode now and include the row in this request chunk.
            let encoded = msg.encode_to_vec();
            debug_assert_eq!(
                encoded.len(),
                size,
                "prost::encoded_len disagrees with encode_to_vec length"
            );

            serialized_rows.push(encoded);
            total_size += size;
            processed_count += 1;
        }

        if serialized_rows.is_empty() {
            return Poll::Ready(None);
        }

        let proto_rows = ProtoRows { serialized_rows };
        let proto_data = ProtoData {
            writer_schema: if *this.include_schema_next {
                Some(this.proto_schema.clone())
            } else {
                None
            },
            rows: Some(proto_rows),
        };

        let append_rows_request = AppendRowsRequest {
            write_stream: this.table_batch.stream_name().to_string(),
            offset: None,
            trace_id: this.trace_id.clone(),
            missing_value_interpretations: HashMap::new(),
            default_missing_value_interpretation: MissingValueInterpretation::Unspecified.into(),
            rows: Some(append_rows_request::Rows::ProtoRows(proto_data)),
        };

        // Track the total bytes being sent using encoded_len
        let request_bytes = append_rows_request.encoded_len();
        this.bytes_sent_counter.fetch_add(request_bytes, Ordering::Relaxed);

        *this.current_index += processed_count;
        // After the first request, avoid sending schema again in this stream
        if *this.include_schema_next {
            *this.include_schema_next = false;
        }

        Poll::Ready(Some(append_rows_request))
    }
}

#[derive(Debug)]
/// Handle returned when a batch append has been accepted by a connection worker.
struct ConnectionWorkerAppendHandle {
    worker_index: usize,
    batch_index: usize,
    stream_name: String,
    response_rx: oneshot::Receiver<BatchAppendResult>,
}

impl ConnectionWorkerAppendHandle {
    /// Waits for the worker to finish the batch, including internal retries.
    async fn wait(self) -> Result<BatchAppendResult, BQError> {
        self.response_rx.await.map_err(|err| {
            BQError::TonicStatusError(Status::unavailable(format!(
                "connection worker response error for worker {} batch {} stream {:?}: {err}",
                self.worker_index, self.batch_index, self.stream_name
            )))
        })
    }
}

/// Creates a configured gRPC client for BigQuery Storage Write API.
async fn create_grpc_client() -> Result<BigQueryWriteClient<Channel>, BQError> {
    // Since Tonic 0.12.0, TLS root certificates are no longer included by default.
    // They must now be specified explicitly.
    // See: https://github.com/hyperium/tonic/pull/1731
    let tls_config = ClientTlsConfig::new()
        .domain_name(BIGQUERY_STORAGE_API_DOMAIN)
        .with_enabled_roots();

    let channel = Channel::from_static(BIG_QUERY_STORAGE_API_URL)
        .tls_config(tls_config)?
        // Configure HTTP/2 keepalive to detect dead connections and prevent
        // proxies from closing idle connections.
        .http2_keep_alive_interval(Duration::from_secs(HTTP2_KEEPALIVE_INTERVAL_SECS))
        .keep_alive_timeout(Duration::from_secs(HTTP2_KEEPALIVE_TIMEOUT_SECS))
        .keep_alive_while_idle(true)
        .connect()
        .await?;

    Ok(BigQueryWriteClient::new(channel)
        .max_encoding_message_size(MAX_MESSAGE_SIZE_BYTES)
        .max_decoding_message_size(MAX_MESSAGE_SIZE_BYTES)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip))
}

/// Mutable state owned by a single connection worker task.
struct ConnectionWorkerState {
    worker_id: usize,
    auth: Arc<dyn Authenticator>,
    grpc_client: Mutex<Option<BigQueryWriteClient<Channel>>>,
}

impl std::fmt::Debug for ConnectionWorkerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionWorkerState")
            .field("worker_id", &self.worker_id)
            .finish()
    }
}

impl ConnectionWorkerState {
    /// Creates a new worker state with no active gRPC connection.
    fn new(worker_id: usize, auth: Arc<dyn Authenticator>) -> Self {
        Self {
            worker_id,
            auth,
            grpc_client: Mutex::new(None),
        }
    }

    /// Drops the current gRPC connection.
    async fn reset_connection(&self) {
        *self.grpc_client.lock().await = None;
    }

    /// Invalidates the current connection and eagerly creates a fresh one.
    async fn invalidate(&self) -> Result<(), Status> {
        self.reset_connection().await;

        match create_grpc_client().await {
            Ok(client) => {
                *self.grpc_client.lock().await = Some(client);

                Ok(())
            }
            Err(err) => Err(Status::unavailable(format!(
                "failed to recreate worker connection: {err}"
            ))),
        }
    }
}

#[async_trait]
trait AppendRowsWorkerRequest: Send {
    async fn run(self: Box<Self>, state: Arc<ConnectionWorkerState>);
}

#[derive(Debug)]
struct AppendRowsJob<M> {
    worker_set: Arc<ConnectionWorkerSetInner>,
    worker_index: usize,
    table_batch: TableBatch<M>,
    batch_index: usize,
    trace_id: String,
    permit: OwnedSemaphorePermit,
    response_tx: oneshot::Sender<BatchAppendResult>,
}

#[async_trait]
impl<M> AppendRowsWorkerRequest for AppendRowsJob<M>
where
    M: Message + Send + Sync + 'static,
{
    async fn run(self: Box<Self>, state: Arc<ConnectionWorkerState>) {
        let Self {
            worker_set,
            worker_index,
            table_batch,
            batch_index,
            trace_id,
            permit,
            response_tx,
        } = *self;

        let worker_id = state.worker_id;
        let result = worker_handle_table_batch_with_retry(state, table_batch, batch_index, trace_id, permit).await;

        // We send back the result one we finish handling the batch.
        if response_tx.send(result).is_err() {
            warn!(worker_id, "failed to send append result from worker");
        }

        // We have to decrement the inflight requests count once we finished, to properly allow the
        // system to load balance.
        worker_set.decrement_inflight(worker_index);
    }
}

/// Messages sent to connection worker tasks.
enum ConnectionWorkerMessage {
    AppendRows(Box<dyn AppendRowsWorkerRequest>),
    Invalidate {
        response_tx: oneshot::Sender<Result<(), Status>>,
    },
    Close,
}

/// Ensures a worker has an active gRPC client, creating one if needed.
async fn ensure_worker_client(state: &ConnectionWorkerState) -> Result<BigQueryWriteClient<Channel>, Status> {
    let mut grpc_client = state.grpc_client.lock().await;
    if grpc_client.is_none() {
        match create_grpc_client().await {
            Ok(client) => {
                *grpc_client = Some(client);
            }
            Err(err) => {
                return Err(Status::unavailable(format!(
                    "failed to create worker connection: {err}"
                )));
            }
        }
    }

    match grpc_client.as_ref() {
        Some(client) => Ok(client.clone()),
        None => Err(Status::unavailable("worker connection unavailable")),
    }
}

/// Executes a single append attempt for a table batch on a worker connection.
async fn worker_handle_table_batch_once<M>(
    state: Arc<ConnectionWorkerState>,
    table_batch: TableBatch<M>,
    batch_index: usize,
    stream_name: &str,
    proto_schema: ProtoSchema,
    bytes_sent_counter: Arc<AtomicUsize>,
    trace_id: String,
) -> WorkerAppendResult
where
    M: Message + 'static,
{
    let mut batch_responses = Vec::new();

    let request_stream = AppendRequestsStream::new(table_batch, proto_schema, bytes_sent_counter, trace_id);
    let request = match StorageApi::new_authorized_request(state.auth.clone(), request_stream).await {
        Ok(request) => request,
        Err(err) => {
            warn!(
                worker_id = state.worker_id,
                batch_index,
                stream_name = %stream_name,
                error = %err,
                "auth error in connection worker"
            );

            batch_responses.push(Err(Status::unauthenticated(err.to_string())));

            return batch_responses;
        }
    };

    // Clone the client before starting the RPC so this worker can multiplex more appends.
    let mut client = match ensure_worker_client(state.as_ref()).await {
        Ok(client) => client,
        Err(status) => {
            batch_responses.push(Err(status));

            return batch_responses;
        }
    };
    let append_result = client.append_rows(request).await;

    match append_result {
        Ok(response) => {
            let mut streaming_response = response.into_inner();

            while let Some(response) = streaming_response.next().await {
                let normalized_response = match response {
                    Ok(response) => normalize_append_response(response, batch_index, stream_name),
                    Err(status) => Err(status),
                };

                if let Err(status) = &normalized_response {
                    warn!(
                        worker_id = state.worker_id,
                        batch_index,
                        stream_name = %stream_name,
                        error = %status,
                        "batch append error response"
                    );
                }

                batch_responses.push(normalized_response);
            }

            ensure_non_empty_batch_responses(&mut batch_responses, batch_index, stream_name);
        }
        Err(status) => {
            warn!(
                worker_id = state.worker_id,
                batch_index,
                stream_name = %stream_name,
                error = %status,
                "failed to append batch in connection worker"
            );

            batch_responses.push(Err(status));
        }
    }

    batch_responses
}

/// Executes a table batch on a worker connection with internal retry/backoff handling.
async fn worker_handle_table_batch_with_retry<M>(
    state: Arc<ConnectionWorkerState>,
    table_batch: TableBatch<M>,
    batch_index: usize,
    trace_id: String,
    _permit: OwnedSemaphorePermit,
) -> BatchAppendResult
where
    M: Message + Send + Sync + 'static,
{
    let stream_name = table_batch.stream_name().to_string();
    let proto_schema = StorageApi::create_proto_schema(table_batch.table_descriptor());
    let bytes_sent_counter = Arc::new(AtomicUsize::new(0));

    let mut batch_responses = Vec::new();

    for attempt in 0..MAX_APPEND_RETRY_ATTEMPTS {
        batch_responses.clear();

        batch_responses = worker_handle_table_batch_once(
            state.clone(),
            table_batch.clone(),
            batch_index,
            &stream_name,
            proto_schema.clone(),
            bytes_sent_counter.clone(),
            trace_id.clone(),
        )
        .await;

        let should_retry = should_retry_batch_responses(&batch_responses);
        if should_retry && attempt < MAX_APPEND_RETRY_ATTEMPTS - 1 {
            let backoff = calculate_append_retry_backoff(attempt);

            warn!(
                worker_id = state.worker_id,
                batch_index,
                stream_name = %stream_name,
                attempt = attempt + 1,
                max_attempts = MAX_APPEND_RETRY_ATTEMPTS,
                backoff_ms = backoff.as_millis(),
                "retrying batch due to retryable append errors in connection worker"
            );

            sleep(backoff).await;

            continue;
        }

        break;
    }

    let bytes_sent = bytes_sent_counter.load(Ordering::Relaxed);
    debug!(
        worker_id = state.worker_id,
        batch_index,
        stream_name = %stream_name,
        bytes_sent,
        "batch completed in connection worker"
    );

    BatchAppendResult::new(batch_index, batch_responses, bytes_sent)
}

/// Background task loop for a single connection worker.
async fn run_connection_worker(
    worker_id: usize,
    auth: Arc<dyn Authenticator>,
    mut rx: mpsc::Receiver<ConnectionWorkerMessage>,
) {
    let state = Arc::new(ConnectionWorkerState::new(worker_id, auth));
    let mut worker_tasks = JoinSet::new();
    let mut should_close = false;

    while !should_close {
        tokio::select! {
            biased;

            Some(result) = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
                if let Err(err) = result {
                    warn!(worker_id, error = %err, "connection worker task failed");
                }
            }

            message = rx.recv() => {
                match message {
                    Some(ConnectionWorkerMessage::AppendRows(request)) => {
                        // Spawn per-batch tasks because one HTTP/2 connection can multiplex many append streams.
                        let task_state = state.clone();
                        worker_tasks.spawn(async move {
                            request.run(task_state).await;
                        });
                    }
                    Some(ConnectionWorkerMessage::Invalidate { response_tx }) => {
                        let worker_id = state.worker_id;
                        let result = state.invalidate().await;
                        if response_tx.send(result).is_err() {
                            warn!(worker_id, "failed to send invalidate ack from worker");
                        }
                    }
                    Some(ConnectionWorkerMessage::Close) | None => {
                        should_close = true;
                    }
                }
            }
        }
    }

    // Before shutting down, we abort all tasks and wait for them to finish to avoid having tasks
    // outlive the worker.
    worker_tasks.abort_all();
    while let Some(result) = worker_tasks.join_next().await {
        if let Err(err) = result {
            debug!(worker_id, error = %err, "connection worker task terminated during shutdown");
        }
    }

    debug!(worker_id, "connection worker closed");
}

/// Internal connection worker state shared across clones.
#[derive(Debug)]
struct ConnectionWorkerSetInner {
    senders: Vec<mpsc::Sender<ConnectionWorkerMessage>>,
    inflight_requests: Vec<AtomicUsize>,
    next_worker: AtomicUsize,
    request_semaphore: Arc<Semaphore>,
}

impl Drop for ConnectionWorkerSetInner {
    fn drop(&mut self) {
        for sender in &self.senders {
            if let Err(err) = sender.try_send(ConnectionWorkerMessage::Close) {
                warn!(error = %err, "failed to close connection worker on worker set drop");
            }
        }
    }
}

impl ConnectionWorkerSetInner {
    /// Returns the current in-flight count for a worker.
    fn inflight(&self, worker_index: usize) -> usize {
        self.inflight_requests[worker_index].load(Ordering::Relaxed)
    }

    /// Increments the in-flight count for a worker.
    fn increment_inflight(&self, worker_index: usize) {
        self.inflight_requests[worker_index].fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the in-flight count for a worker.
    fn decrement_inflight(&self, worker_index: usize) {
        self.inflight_requests[worker_index].fetch_sub(1, Ordering::Relaxed);
    }

    /// Selects a worker using the power-of-two-choices strategy.
    ///
    /// This is a small improvement over pure round-robin to distribute work better
    /// when some workers are temporarily slower or stuck. Selection stays O(1),
    /// and the in-flight counters may be slightly stale, which is acceptable here.
    ///
    /// This is not optimal because it does not account for batch size or expected
    /// retry cost, only the number of currently in-flight jobs. That is sufficient
    /// for now and keeps dispatch cheap.
    fn select_worker_index(&self) -> usize {
        let worker_count = self.senders.len();
        if worker_count <= 1 {
            return 0;
        }

        let first_worker_index = self.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        let second_worker_index = self.next_worker.fetch_add(1, Ordering::Relaxed) % worker_count;
        let first_inflight = self.inflight(first_worker_index);
        let second_inflight = self.inflight(second_worker_index);

        if first_inflight <= second_inflight {
            first_worker_index
        } else {
            second_worker_index
        }
    }
}

/// Shared set of long-lived connection workers.
#[derive(Debug, Clone)]
struct ConnectionWorkerSet {
    inner: Arc<ConnectionWorkerSetInner>,
}

impl ConnectionWorkerSet {
    /// Spawns a fixed number of connection workers.
    fn new(worker_count: usize, auth: Arc<dyn Authenticator>, max_inflight_requests: usize) -> Self {
        let worker_count = worker_count.max(1);

        let mut senders = Vec::with_capacity(worker_count);
        let mut inflight_requests = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (tx, rx) = mpsc::channel(CONNECTION_WORKER_CHANNEL_CAPACITY);
            senders.push(tx);
            inflight_requests.push(AtomicUsize::new(0));

            tokio::spawn(run_connection_worker(worker_id, auth.clone(), rx));
        }

        Self {
            inner: Arc::new(ConnectionWorkerSetInner {
                senders,
                inflight_requests,
                next_worker: AtomicUsize::new(0),
                request_semaphore: Arc::new(Semaphore::new(max_inflight_requests)),
            }),
        }
    }

    /// Dispatches a table batch to one worker selected in round-robin order.
    async fn append_table_batch<M>(
        &self,
        table_batch: TableBatch<M>,
        batch_index: usize,
        trace_id: String,
    ) -> Result<ConnectionWorkerAppendHandle, BQError>
    where
        M: Message + Send + Sync + 'static,
    {
        if self.inner.senders.is_empty() {
            return Err(BQError::TonicStatusError(Status::unavailable(
                "no connection workers available",
            )));
        }

        let permit = self.inner.request_semaphore.clone().acquire_owned().await?;
        let worker_index = self.inner.select_worker_index();
        let sender = self.inner.senders[worker_index].clone();
        let stream_name = table_batch.stream_name().to_string();

        // This tracks admitted jobs per worker, not bytes or request chunk count.
        self.inner.increment_inflight(worker_index);

        let (response_tx, response_rx) = oneshot::channel();
        let message = ConnectionWorkerMessage::AppendRows(Box::new(AppendRowsJob {
            worker_set: self.inner.clone(),
            worker_index,
            table_batch,
            batch_index,
            trace_id,
            permit,
            response_tx,
        }));

        if let Err(err) = sender.send(message).await {
            // We have to decrement if the send failed, since no work will be done.
            self.inner.decrement_inflight(worker_index);

            warn!(
                worker_index,
                batch_index,
                stream_name = %stream_name,
                error = %err,
                "failed to send append request to connection worker"
            );

            return Err(BQError::TonicStatusError(Status::unavailable(format!(
                "connection worker send error: {err}"
            ))));
        }

        Ok(ConnectionWorkerAppendHandle {
            worker_index,
            batch_index,
            stream_name,
            response_rx,
        })
    }

    /// Invalidates and recreates the connection in each worker.
    async fn invalidate_all(&self) {
        let mut join_set = JoinSet::new();

        for (worker_index, sender) in self.inner.senders.iter().cloned().enumerate() {
            join_set.spawn(async move {
                let (response_tx, response_rx) = oneshot::channel();
                sender
                    .send(ConnectionWorkerMessage::Invalidate { response_tx })
                    .await
                    .map_err(|err| Status::unavailable(format!("failed to send invalidate message: {err}")))?;

                response_rx
                    .await
                    .map_err(|err| Status::unavailable(format!("failed to receive invalidate ack: {err}")))??;

                Ok::<usize, Status>(worker_index)
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(worker_index)) => {
                    debug!(worker_index, "invalidated connection worker");
                }
                Ok(Err(status)) => {
                    warn!(error = %status, "failed to invalidate connection worker");
                }
                Err(err) => {
                    warn!(error = %err, "connection worker invalidate task failed");
                }
            }
        }
    }
}

/// High-level client for BigQuery Storage Write API operations.
#[derive(Clone)]
pub struct StorageApi {
    /// Shared set of connection workers for append operations.
    connection_workers: ConnectionWorkerSet,
    /// Authentication provider for API requests.
    auth: Arc<dyn Authenticator>,
    /// Base URL for BigQuery API endpoints.
    base_url: String,
}

impl std::fmt::Debug for StorageApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageApi")
            .field("connection_workers", &self.connection_workers)
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl StorageApi {
    /// Creates a new storage API client instance with a custom configuration.
    pub(crate) async fn with_config(auth: Arc<dyn Authenticator>, config: StorageApiConfig) -> Result<Self, BQError> {
        let worker_count = config.connection_pool_size.max(1);
        let connection_workers = ConnectionWorkerSet::new(worker_count, auth.clone(), config.max_inflight_requests);

        Ok(Self {
            connection_workers,
            auth,
            base_url: BIG_QUERY_V2_URL.to_string(),
        })
    }

    /// Configures a custom base URL for BigQuery API endpoints.
    ///
    /// Primarily used for testing scenarios with mock or alternative
    /// BigQuery API endpoints. Returns a mutable reference for chaining.
    pub(crate) fn with_base_url(&mut self, base_url: String) -> &mut Self {
        self.base_url = base_url;
        self
    }

    /// Encodes message rows into protobuf format with size management.
    ///
    /// Processes as many rows as possible while respecting the specified
    /// size limit. Returns the encoded protobuf data and the count of
    /// rows successfully processed. When the returned count is less than
    /// the input slice length, additional calls are required for remaining rows.
    ///
    /// The size limit should be below 10MB to accommodate request metadata
    /// overhead; 9MB provides a safe margin.
    pub fn create_rows<M: Message>(
        table_descriptor: &TableDescriptor,
        rows: &[M],
        max_size_bytes: usize,
    ) -> (append_rows_request::Rows, usize) {
        let proto_schema = Self::create_proto_schema(table_descriptor);

        let mut serialized_rows = Vec::new();
        let mut total_size = 0;

        for row in rows {
            // Use encoded_len to avoid encoding a row that won't fit.
            let row_size = row.encoded_len();
            if total_size + row_size > max_size_bytes {
                break;
            }

            let encoded_row = row.encode_to_vec();
            debug_assert_eq!(
                encoded_row.len(),
                row_size,
                "prost::encoded_len disagrees with encode_to_vec length"
            );

            serialized_rows.push(encoded_row);
            total_size += row_size;
        }

        let num_rows_processed = serialized_rows.len();

        let proto_rows = ProtoRows { serialized_rows };

        let proto_data = ProtoData {
            writer_schema: Some(proto_schema),
            rows: Some(proto_rows),
        };

        (append_rows_request::Rows::ProtoRows(proto_data), num_rows_processed)
    }

    /// Retrieves metadata for a BigQuery write stream.
    ///
    /// Fetches stream information including schema definition and state
    /// details according to the specified view level. Higher view levels
    /// provide more comprehensive information but may have higher latency.
    pub async fn get_write_stream(
        &mut self,
        stream_name: &StreamName,
        view: WriteStreamView,
    ) -> Result<WriteStream, BQError> {
        let stream_name_str = stream_name.to_string();

        let get_write_stream_request = GetWriteStreamRequest {
            name: stream_name_str.clone(),
            view: view.into(),
        };

        let request = Self::new_authorized_request(self.auth.clone(), get_write_stream_request).await?;
        let mut grpc_client = create_grpc_client().await?;

        grpc_client
            .get_write_stream(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| {
                warn!(stream_name = %stream_name_str, error = %e, "failed to fetch write stream metadata");
                e.into()
            })
    }

    /// Appends rows to a BigQuery table using the Storage Write API.
    ///
    /// Transmits the provided rows to the specified stream and returns
    /// a streaming response for processing results.
    pub async fn append_rows(
        &mut self,
        stream_name: &StreamName,
        rows: append_rows_request::Rows,
        trace_id: String,
    ) -> Result<Streaming<AppendRowsResponse>, BQError> {
        let stream_name_str = stream_name.to_string();

        let append_rows_request = AppendRowsRequest {
            write_stream: stream_name_str.clone(),
            offset: None,
            trace_id,
            missing_value_interpretations: HashMap::new(),
            default_missing_value_interpretation: MissingValueInterpretation::Unspecified.into(),
            rows: Some(rows),
        };

        let request =
            Self::new_authorized_request(self.auth.clone(), tokio_stream::iter(vec![append_rows_request])).await?;
        let mut grpc_client = create_grpc_client().await?;

        grpc_client
            .append_rows(request)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| {
                warn!(stream_name = %stream_name_str, error = %e, "failed to append rows");
                e.into()
            })
    }

    /// Appends rows from multiple table batches with concurrent processing.
    ///
    /// Returns a collection of batch results containing
    /// responses, metadata, and bytes sent for each batch processed. Results are
    /// ordered by completion, not by submission; use `BatchAppendResult::batch_index`
    /// to correlate with the original input order.
    ///
    /// Each table batch will result in its own AppendRequests gRPC request and if a batch exceeds
    /// the limit of 10mb, it will be split into multiple requests automatically.
    pub async fn append_table_batches<M, I>(&self, append_requests: I) -> Result<Vec<BatchAppendResult>, BQError>
    where
        M: Message + Send + Sync + 'static,
        I: IntoIterator<Item = BatchAppendRequest<M>>,
        I::IntoIter: ExactSizeIterator,
    {
        let append_requests = append_requests.into_iter();
        let batches_num = append_requests.len();

        if batches_num == 0 {
            return Ok(Vec::new());
        }

        let mut handles = Vec::with_capacity(batches_num);
        for (idx, append_request) in append_requests.enumerate() {
            let handle = self
                .connection_workers
                .append_table_batch(append_request.table_batch, idx, append_request.trace_id)
                .await?;
            handles.push(handle);
        }

        let mut pending_results = FuturesUnordered::new();
        for handle in handles {
            pending_results.push(handle.wait());
        }

        let mut batch_results = Vec::with_capacity(batches_num);
        while let Some(batch_result) = pending_results.next().await {
            batch_results.push(batch_result?);
        }

        Ok(batch_results)
    }

    /// Invalidates all worker connections.
    ///
    /// Sends an invalidate message to each worker, which drops its current
    /// connection and recreates it before processing subsequent requests.
    /// Call this after DDL changes to ensure writes use fresh connections.
    pub async fn invalidate_all_connections(&self) {
        self.connection_workers.invalidate_all().await;
    }

    /// Creates an authenticated gRPC request with Bearer token authorization.
    ///
    /// Retrieves an access token from the authenticator and attaches it
    /// as a Bearer token in the request authorization header. Used by
    /// all Storage Write API operations requiring authentication.
    async fn new_authorized_request<T>(auth: Arc<dyn Authenticator>, message: T) -> Result<Request<T>, BQError> {
        let access_token = auth.access_token().await?;
        let bearer_token = format!("Bearer {access_token}");

        Self::new_authorized_request_with_bearer_token(bearer_token, message)
    }

    /// Creates an authenticated gRPC request with a precomputed Bearer token.
    fn new_authorized_request_with_bearer_token<T>(bearer_token: String, message: T) -> Result<Request<T>, BQError> {
        let bearer_value = bearer_token.as_str().try_into()?;

        let mut request = Request::new(message);
        let meta = request.metadata_mut();
        meta.insert("authorization", bearer_value);

        Ok(request)
    }

    /// Converts table field descriptors to protobuf field descriptors.
    ///
    /// Transforms the high-level field descriptors into the protobuf
    /// format required by BigQuery Storage Write API schema definitions.
    /// Maps column types and modes to their protobuf equivalents.
    fn create_field_descriptors(table_descriptor: &TableDescriptor) -> Vec<FieldDescriptorProto> {
        table_descriptor
            .field_descriptors
            .iter()
            .map(|fd| {
                let typ: Type = fd.typ.into();
                let label: Label = fd.mode.into();

                FieldDescriptorProto {
                    name: Some(fd.name.clone()),
                    number: Some(fd.number as i32),
                    label: Some(label.into()),
                    r#type: Some(typ.into()),
                    type_name: None,
                    extendee: None,
                    default_value: None,
                    oneof_index: None,
                    json_name: None,
                    options: None,
                    proto3_optional: None,
                }
            })
            .collect()
    }

    /// Creates a protobuf descriptor from field descriptors.
    ///
    /// Wraps field descriptors in a [`DescriptorProto`] structure with
    /// the standard table schema name. Used as an intermediate step
    /// in protobuf schema generation.
    fn create_proto_descriptor(field_descriptors: Vec<FieldDescriptorProto>) -> DescriptorProto {
        DescriptorProto {
            name: Some("table_schema".to_string()),
            field: field_descriptors,
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        }
    }

    /// Generates a complete protobuf schema from table descriptor.
    ///
    /// Creates the final [`ProtoSchema`] structure containing all
    /// field definitions required for BigQuery Storage Write API
    /// operations. This schema is included in append requests.
    fn create_proto_schema(table_descriptor: &TableDescriptor) -> ProtoSchema {
        let field_descriptors = Self::create_field_descriptors(table_descriptor);
        let proto_descriptor = Self::create_proto_descriptor(field_descriptors);

        ProtoSchema {
            proto_descriptor: Some(proto_descriptor),
        }
    }
}

#[cfg(test)]
pub mod test {
    use prost::Message;
    use std::time::{Duration, SystemTime};
    use tokio_stream::StreamExt;
    use tonic::Status;

    use crate::google::cloud::bigquery::storage::v1::{append_rows_response, AppendRowsResponse, RowError};
    use crate::google::rpc::Status as GoogleRpcStatus;
    use crate::model::dataset::Dataset;
    use crate::model::field_type::FieldType;
    use crate::model::table::Table;
    use crate::model::table_field_schema::TableFieldSchema;
    use crate::model::table_schema::TableSchema;
    use crate::storage::{
        ensure_non_empty_batch_responses, is_retryable_append_status, normalize_append_response,
        should_retry_batch_responses, BatchAppendRequest, BatchAppendResult, ColumnMode, ColumnType, FieldDescriptor,
        StorageApi, StreamName, TableBatch, TableDescriptor,
    };
    use crate::{env_vars, Client};

    #[derive(Clone, PartialEq, Message)]
    struct Actor {
        #[prost(int32, tag = "1")]
        actor_id: i32,
        #[prost(string, tag = "2")]
        first_name: String,
        #[prost(string, tag = "3")]
        last_name: String,
        #[prost(string, tag = "4")]
        last_update: String,
    }

    fn create_test_table_descriptor() -> TableDescriptor {
        let field_descriptors = vec![
            FieldDescriptor {
                name: "actor_id".to_string(),
                number: 1,
                typ: ColumnType::Int64,
                mode: ColumnMode::Nullable,
            },
            FieldDescriptor {
                name: "first_name".to_string(),
                number: 2,
                typ: ColumnType::String,
                mode: ColumnMode::Nullable,
            },
            FieldDescriptor {
                name: "last_name".to_string(),
                number: 3,
                typ: ColumnType::String,
                mode: ColumnMode::Nullable,
            },
            FieldDescriptor {
                name: "last_update".to_string(),
                number: 4,
                typ: ColumnType::String,
                mode: ColumnMode::Nullable,
            },
        ];

        TableDescriptor { field_descriptors }
    }

    async fn setup_test_table(
        client: &mut Client,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        client.dataset().delete_if_exists(project_id, dataset_id, true).await;

        let created_dataset = client.dataset().create(Dataset::new(project_id, dataset_id)).await?;
        assert_eq!(created_dataset.id, Some(format!("{project_id}:{dataset_id}")));

        let table = Table::new(
            project_id,
            dataset_id,
            table_id,
            TableSchema::new(vec![
                TableFieldSchema::new("actor_id", FieldType::Int64),
                TableFieldSchema::new("first_name", FieldType::String),
                TableFieldSchema::new("last_name", FieldType::String),
                TableFieldSchema::new("last_update", FieldType::Timestamp),
            ]),
        );
        let created_table = client
            .table()
            .create(
                table
                    .description("A table used for unit tests")
                    .label("owner", "me")
                    .label("env", "prod")
                    .expiration_time(SystemTime::now() + Duration::from_secs(3600)),
            )
            .await?;
        assert_eq!(created_table.table_reference.table_id, table_id.to_string());

        Ok(())
    }

    fn create_test_actor(id: i32, first_name: &str) -> Actor {
        Actor {
            actor_id: id,
            first_name: first_name.to_string(),
            last_name: "Doe".to_string(),
            last_update: "2007-02-15 09:34:33 UTC".to_string(),
        }
    }

    async fn call_append_rows(
        client: &mut Client,
        table_descriptor: &TableDescriptor,
        stream_name: &StreamName,
        mut rows: &[Actor],
        max_size: usize,
    ) -> Result<u8, Box<dyn std::error::Error>> {
        // This loop is needed because the AppendRows API has a payload size limit of 10MB and the create_rows
        // function may not process all the rows in the rows slice due to the 10MB limit. Even though in this
        // example we are only sending two rows (which won't breach the 10MB limit), in a real-world scenario,
        // we may have to send more rows and the loop will be needed to process all the rows.
        let mut num_append_rows_calls = 0;
        loop {
            let (encoded_rows, num_processed) = StorageApi::create_rows(table_descriptor, rows, max_size);
            let mut streaming = client
                .storage_mut()
                .append_rows(stream_name, encoded_rows, "test-trace-id".to_string())
                .await?;

            num_append_rows_calls += 1;

            while let Some(response) = streaming.next().await {
                response?;
            }

            // All the rows have been processed
            if num_processed == rows.len() {
                break;
            }

            // Process the remaining rows
            rows = &rows[num_processed..];
        }

        Ok(num_append_rows_calls)
    }

    #[tokio::test]
    async fn test_append_rows() {
        let (ref project_id, ref dataset_id, ref table_id, ref sa_key) = env_vars();
        let dataset_id = &format!("{dataset_id}_storage");

        let mut client = Client::from_service_account_key_file(sa_key).await.unwrap();

        setup_test_table(&mut client, project_id, dataset_id, table_id)
            .await
            .unwrap();

        let table_descriptor = create_test_table_descriptor();
        let actor1 = create_test_actor(1, "John");
        let actor2 = create_test_actor(2, "Jane");

        let stream_name = StreamName::new_default(project_id.clone(), dataset_id.clone(), table_id.clone());
        let rows: &[Actor] = &[actor1, actor2];

        let max_size = 9 * 1024 * 1024; // 9 MB
        let num_append_rows_calls = call_append_rows(&mut client, &table_descriptor, &stream_name, rows, max_size)
            .await
            .unwrap();
        assert_eq!(num_append_rows_calls, 1);

        // It was found after experimenting that one row in this test encodes to about 38 bytes
        // We artificially limit the size of the rows to test that the loop processes all the rows
        let max_size = 50; // 50 bytes
        let num_append_rows_calls = call_append_rows(&mut client, &table_descriptor, &stream_name, rows, max_size)
            .await
            .unwrap();
        assert_eq!(num_append_rows_calls, 2);
    }

    #[tokio::test]
    async fn test_append_table_batches() {
        let (ref project_id, ref dataset_id, ref table_id, ref sa_key) = env_vars();
        let dataset_id = &format!("{dataset_id}_storage_table_batches");

        let mut client = Client::from_service_account_key_file(sa_key).await.unwrap();

        setup_test_table(&mut client, project_id, dataset_id, table_id)
            .await
            .unwrap();

        let table_descriptor = create_test_table_descriptor();
        let stream_name = StreamName::new_default(project_id.clone(), dataset_id.clone(), table_id.clone());
        // Create multiple table batches (all targeting the same table in this test)
        let batch1 = TableBatch::new(
            stream_name.clone(),
            table_descriptor.clone(),
            vec![
                create_test_actor(1, "John"),
                create_test_actor(2, "Jane"),
                create_test_actor(3, "Bob"),
                create_test_actor(4, "Alice"),
            ],
        );

        let batch2 = TableBatch::new(
            stream_name.clone(),
            table_descriptor.clone(),
            vec![create_test_actor(5, "Charlie"), create_test_actor(6, "Dave")],
        );

        let batch3 = TableBatch::new(stream_name, table_descriptor, vec![create_test_actor(7, "Eve")]);

        let table_batches = vec![batch1, batch2, batch3];

        // Test that all batches are processed using the default max_concurrent_requests from config.
        let batch_responses = client
            .storage_mut()
            .append_table_batches(
                table_batches
                    .into_iter()
                    .map(|table_batch| BatchAppendRequest::new(table_batch, "test-trace-id".to_string())),
            )
            .await
            .unwrap();

        // We expect 3 responses per batch (one for each batch)
        assert_eq!(batch_responses.len(), 3);

        // Verify all responses are successful and track total bytes sent.
        let mut total_bytes_across_all_batches = 0;
        for batch_result in batch_responses {
            // Verify each individual response for detailed error reporting.
            for response in &batch_result.responses {
                assert!(response.is_ok(), "Response should be successful: {response:?}");
            }

            // Verify that some bytes were sent (should be greater than 0).
            let bytes_sent = batch_result.bytes_sent;
            assert!(
                bytes_sent > 0,
                "Bytes sent should be greater than 0 for batch {}, got: {}",
                batch_result.batch_index,
                bytes_sent
            );

            total_bytes_across_all_batches += bytes_sent;
        }

        // Verify that we sent bytes across all batches
        assert!(
            total_bytes_across_all_batches > 0,
            "Total bytes sent across all batches should be greater than 0"
        );
    }

    #[test]
    fn test_ensure_non_empty_batch_responses_adds_retryable_error_on_empty_stream() {
        let mut batch_responses = Vec::new();

        ensure_non_empty_batch_responses(
            &mut batch_responses,
            0,
            "projects/test/datasets/test/tables/test/streams/_default",
        );

        assert_eq!(batch_responses.len(), 1);
        let err = batch_responses
            .pop()
            .expect("missing inserted error")
            .expect_err("expected request error");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(err.message(), "append response stream ended without responses");
    }

    #[test]
    fn test_ensure_non_empty_batch_responses_keeps_existing_responses() {
        let mut batch_responses = vec![Ok(AppendRowsResponse::default())];

        ensure_non_empty_batch_responses(
            &mut batch_responses,
            0,
            "projects/test/datasets/test/tables/test/streams/_default",
        );

        assert_eq!(batch_responses.len(), 1);
        assert!(batch_responses[0].is_ok());
    }

    #[test]
    fn test_is_retryable_append_status_expected_codes() {
        assert!(is_retryable_append_status(&Status::unavailable("unavailable")));
        assert!(is_retryable_append_status(&Status::internal("internal")));
        assert!(is_retryable_append_status(&Status::aborted("aborted")));
        assert!(is_retryable_append_status(&Status::cancelled("cancelled")));
        assert!(is_retryable_append_status(&Status::deadline_exceeded("deadline")));
        assert!(is_retryable_append_status(&Status::resource_exhausted("quota")));
        assert!(is_retryable_append_status(&Status::unknown("transport error")));
        assert!(is_retryable_append_status(&Status::unknown("connection reset")));
        assert!(is_retryable_append_status(&Status::failed_precondition(
            "Closing the stream because it has been inactive"
        )));

        assert!(!is_retryable_append_status(&Status::unknown("some unknown cause")));
        assert!(!is_retryable_append_status(&Status::invalid_argument("invalid")));
        assert!(!is_retryable_append_status(&Status::failed_precondition(
            "failed precondition"
        )));
    }

    #[test]
    fn test_normalize_append_response_converts_error_and_missing_outcome_to_status() {
        let error_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: Some(append_rows_response::Response::Error(GoogleRpcStatus {
                code: tonic::Code::Internal as i32,
                message: "internal error".to_string(),
                details: Vec::new(),
            })),
        };
        let normalized = normalize_append_response(
            error_response,
            0,
            "projects/test/datasets/test/tables/test/streams/_default",
        );
        let status = normalized.expect_err("expected normalized error");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");

        let missing_outcome_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: None,
        };
        let normalized = normalize_append_response(
            missing_outcome_response,
            0,
            "projects/test/datasets/test/tables/test/streams/_default",
        );
        let status = normalized.expect_err("expected missing outcome to be error");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "append response missing append_result/error outcome");
    }

    #[test]
    fn test_should_retry_batch_responses_only_for_retryable_request_errors() {
        assert!(should_retry_batch_responses(&[Err(Status::unavailable("retry"))]));

        assert!(!should_retry_batch_responses(&[
            Err(Status::unavailable("retry")),
            Err(Status::invalid_argument("do not retry")),
        ]));

        let success_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: Some(append_rows_response::Response::AppendResult(
                append_rows_response::AppendResult { offset: None },
            )),
        };
        assert!(!should_retry_batch_responses(&[
            Ok(success_response),
            Err(Status::unavailable("retry")),
        ]));

        let row_error_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: vec![RowError::default()],
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: Some(append_rows_response::Response::AppendResult(
                append_rows_response::AppendResult { offset: None },
            )),
        };
        assert!(!should_retry_batch_responses(&[Ok(row_error_response)]));
    }

    #[test]
    fn test_is_retryable_append_status_matches_java_managed_writer_retry_set() {
        assert!(is_retryable_append_status(&Status::aborted("retry")));
        assert!(is_retryable_append_status(&Status::unavailable("retry")));
        assert!(is_retryable_append_status(&Status::cancelled("retry")));
        assert!(is_retryable_append_status(&Status::internal("retry")));
        assert!(is_retryable_append_status(&Status::deadline_exceeded("retry")));
        assert!(is_retryable_append_status(&Status::resource_exhausted("retry")));
        assert!(is_retryable_append_status(&Status::unknown("transport failure")));
        assert!(is_retryable_append_status(&Status::unknown("connection closed")));

        assert!(!is_retryable_append_status(&Status::unknown("do not retry by default")));
        assert!(!is_retryable_append_status(&Status::invalid_argument("do not retry")));
        assert!(!is_retryable_append_status(&Status::failed_precondition(
            "do not retry"
        )));
        assert!(!is_retryable_append_status(&Status::unauthenticated("do not retry")));
    }

    #[test]
    fn test_batch_append_result_is_success_requires_append_result_and_no_row_errors() {
        let success_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: Some(append_rows_response::Response::AppendResult(
                append_rows_response::AppendResult { offset: None },
            )),
        };
        let batch_result = BatchAppendResult::new(0, vec![Ok(success_response)], 10);
        assert!(batch_result.is_success());

        let missing_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: None,
        };
        let batch_result = BatchAppendResult::new(0, vec![Ok(missing_response)], 10);
        assert!(!batch_result.is_success());

        let row_error_response = AppendRowsResponse {
            updated_schema: None,
            row_errors: vec![RowError::default()],
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: Some(append_rows_response::Response::AppendResult(
                append_rows_response::AppendResult { offset: None },
            )),
        };
        let batch_result = BatchAppendResult::new(0, vec![Ok(row_error_response)], 10);
        assert!(!batch_result.is_success());
    }
}
