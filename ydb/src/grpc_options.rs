use std::{fs, io, path::Path, sync::Arc, time::Duration};

use tonic::transport::{Certificate, ClientTlsConfig};
use tracing::trace;

use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;

/// Common options for gRPC connections.
/// ```
#[derive(Debug, Clone)]
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

impl GrpcOptions {
    /// Sets interval between HTTP/2 PING-based keepalives.
    pub fn keepalive_interval<I: Into<Option<Duration>>>(mut self, value: I) -> Self {
        self.keepalive_interval = value.into();
        self
    }

    /// Loads TLS certificate from given path.
    pub fn load_certificate<P: AsRef<Path>>(mut self, path: P) -> io::Result<Self> {
        let pem = fs::read_to_string(path)?;
        trace!("loaded cert: {pem}");

        let ca = Certificate::from_pem(pem);
        self.tls_config = Some(ClientTlsConfig::new().ca_certificate(ca).into());

        Ok(self)
    }

    /// Sets TLS configuration.
    pub fn tls_config<I: Into<Arc<ClientTlsConfig>>>(mut self, value: I) -> Self {
        self.tls_config = Some(value.into());
        self
    }

    /// Sets maximum gRPC message size.
    pub fn max_message_size(mut self, value: usize) -> Self {
        self.max_message_size = value;
        self
    }
}

/// Helper trait for types that has configurable gRPC options.
pub trait HasGrpcOptions {
    /// Modifies the inner gRPC options.
    ///
    /// For fallible version of this method, see [`Self::try_with_grpc_opts`].
    ///
    /// ```
    /// # use std::time::Duration;
    /// #
    /// # fn main() {
    ///     let client_builder = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")
    ///         .with_grpc_opts(|opts| opts.keepalive_interval(Duration::from_sec(10)));
    /// # }
    /// ```
    fn with_grpc_opts<F: FnOnce(GrpcOptions) -> GrpcOptions>(mut self, f: F) -> Self
    where
        Self: Sized,
    {
        *self.grpc_opts_mut() = f(std::mem::take(self.grpc_opts_mut()));
        self
    }

    /// Tries to modify the inner gRPC options.
    ///
    /// Fallible counterpart of [`Self::with_grpc_opts`].
    fn try_with_grpc_opts<E, F: FnOnce(GrpcOptions) -> Result<GrpcOptions, E>>(
        mut self,
        f: F,
    ) -> Result<Self, E>
    where
        Self: Sized,
    {
        *self.grpc_opts_mut() = f(std::mem::take(self.grpc_opts_mut()))?;
        Ok(self)
    }

    /// Borrows the inner gRPC options.
    fn grpc_opts(&self) -> &GrpcOptions;

    /// Mutably borrows the inner gRPC options.
    fn grpc_opts_mut(&mut self) -> &mut GrpcOptions;
}
