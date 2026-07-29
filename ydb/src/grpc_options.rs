use std::{fs, io, path::Path, sync::Arc, time::Duration};

use tonic::transport::{Certificate, ClientTlsConfig};
use tracing::trace;

use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;

/// Common options for gRPC connections.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct GrpcOptions {
    /// Interval between HTTP/2 PING-based keepalives.
    pub keepalive_interval: Option<Duration>,
    /// TLS configuration.
    pub tls_config: Option<Arc<ClientTlsConfig>>,
    /// Set the maximum size in bytes for encoded and decoded gRPC messages.
    pub max_message_size: usize,
}

impl Default for GrpcOptions {
    fn default() -> Self {
        Self {
            keepalive_interval: Some(Duration::from_secs(10)),
            tls_config: None,
            max_message_size: DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
        }
    }
}

/// Helper trait for types that has configurable gRPC options.
pub trait HasGrpcOptions {
    /// Sets interval between HTTP/2 PING-based keepalives.
    fn with_grpc_keepalive_interval<I: Into<Option<Duration>>>(mut self, value: I) -> Self
    where
        Self: Sized,
    {
        self.grpc_opts_mut().keepalive_interval = value.into();
        self
    }

    /// Loads TLS certificate from given path.
    fn load_certificate<P: AsRef<Path>>(self, path: P) -> io::Result<Self>
    where
        Self: Sized,
    {
        let pem = fs::read_to_string(path)?;
        trace!("loaded cert: {pem}");

        let ca = Certificate::from_pem(pem);

        Ok(self.with_tls_config(ClientTlsConfig::new().ca_certificate(ca)))
    }

    /// Sets TLS configuration.
    fn with_tls_config<I: Into<Arc<ClientTlsConfig>>>(mut self, value: I) -> Self
    where
        Self: Sized,
    {
        self.grpc_opts_mut().tls_config = Some(value.into());
        self
    }

    /// Sets maximum gRPC message size.
    fn with_grpc_max_message_size(mut self, value: usize) -> Self
    where
        Self: Sized,
    {
        self.grpc_opts_mut().max_message_size = value;
        self
    }

    /// Sets gRPC options.
    fn with_grpc_opts(mut self, opts: GrpcOptions) -> Self
    where
        Self: Sized,
    {
        *self.grpc_opts_mut() = opts;
        self
    }

    /// Mutably borrows the inner gRPC options.
    fn grpc_opts_mut(&mut self) -> &mut GrpcOptions;
}
