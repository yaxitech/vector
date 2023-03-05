use std::{fmt::Debug, num::NonZeroUsize};

use bytes::Bytes;
use codecs::{encoding::Framer, CharacterDelimitedEncoder, JsonSerializerConfig};
use futures::{future, stream::BoxStream, FutureExt, StreamExt};
use http::StatusCode;
use tower::{Service, ServiceBuilder};
use vector_common::request_metadata::RequestMetadata;
use vector_config::configurable_component;
use vector_core::{
    config::{AcknowledgementsConfig, Input},
    event::{Event, EventFinalizers, Finalizable},
    sink::{StreamSink, VectorSink},
};
use vrl::value::Kind;

use crate::{
    codecs::{Encoder, Transformer},
    config::{SinkConfig, SinkContext},
    http::HttpClient,
    internal_events::SinkRequestBuildError,
    schema,
    sinks::{
        azure_monitor_logs_dce::{
            auth::AzureAuthenticator,
            service::{
                AzureMonitorLogsDceRequest, AzureMonitorLogsDceResponse, AzureMonitorLogsDceService,
            },
        },
        prelude::{BatcherSettings, DriverResponse, SinkBatchSettings},
        util::{
            metadata::RequestMetadataBuilder,
            request_builder::EncodeResult,
            retries::{RetryAction, RetryLogic},
            BatchConfig, Compression, RequestBuilder, SinkBuilderExt, TowerRequestConfig,
        },
        Healthcheck,
    },
    tls::{TlsConfig, TlsSettings},
};

impl_generate_config_from_default!(AzureMonitorLogsDceConfig);

/// Authentication credentials using a client secret that was generated for an App Registration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct AzureClientSecretCredentials {
    /// Tenant ID
    pub tenant_id: String,
    /// Client ID
    pub client_id: String,
    /// Client secret
    pub client_secret: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LogsIngestionDefaultBatchSettings;

// Logs Ingestion API has a 1MB limit[1] for API calls.
//
// [1]: https://learn.microsoft.com/en-us/azure/azure-monitor/service-limits#logs-ingestion-api
impl SinkBatchSettings for LogsIngestionDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(1_000_000);
    const TIMEOUT_SECS: f64 = 10.0;
}

/// Configuration for the `azure_monitor_logs_dce` sink.
#[configurable_component(sink("azure_monitor_logs_dce"))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct AzureMonitorLogsDceConfig {
    /// Immutable ID of the Data Collection Rule
    #[configurable(metadata(docs::examples = "dcr-b74e0d383fc9415abaa584ec41adece3"))]
    pub immutable_id: String,

    /// Stream name inside the Data Collection Rule
    #[configurable(metadata(docs::examples = "Custom-CustomTableRawData"))]
    pub stream_name: String,

    /// Logs ingestion endpoint
    #[configurable(metadata(docs::examples = "sample.westeurope.ingest.monitor.azure.com"))]
    pub endpoint_host: String,

    #[configurable(derived)]
    #[serde(default)]
    pub encoding: Transformer,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<LogsIngestionDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    pub client_credentials: Option<AzureClientSecretCredentials>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

impl Default for AzureMonitorLogsDceConfig {
    fn default() -> Self {
        Self {
            immutable_id: "dcr-00000000000000000000000000000000".to_string(),
            stream_name: "Custom-CustomTableRawData".to_string(),
            endpoint_host: "sample.westeurope.ingest.monitor.azure.com".to_string(),
            encoding: Default::default(),
            batch: Default::default(),
            request: Default::default(),
            tls: None,
            client_credentials: None,
            acknowledgements: Default::default(),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "azure_monitor_logs_dce")]
impl SinkConfig for AzureMonitorLogsDceConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let creds = AzureAuthenticator::new(self.client_credentials.as_ref()).await?;

        let tls = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls, cx.proxy())?;

        let healthcheck = future::ok(()).boxed();
        creds.spawn_regenerate_token();
        let sink = self.build_sink(client, creds)?;

        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        let schema_requirement =
            schema::Requirement::empty().required_meaning("timestamp", Kind::timestamp());
        Input::log().with_schema_requirement(schema_requirement)
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl AzureMonitorLogsDceConfig {
    fn build_sink(
        &self,
        client: HttpClient,
        creds: AzureAuthenticator,
    ) -> crate::Result<VectorSink> {
        use crate::sinks::util::service::ServiceBuilderExt;

        let request = self.request.unwrap_with(&TowerRequestConfig {
            rate_limit_num: Some(1000),
            ..Default::default()
        });

        let svc = ServiceBuilder::new()
            .settings(request, AzureMonitorLogsDceRetryLogic)
            .service(AzureMonitorLogsDceService::new(
                client,
                self.create_endpoint(),
                creds,
            ));

        let encoder = Encoder::<Framer>::new(
            CharacterDelimitedEncoder::new(b',').into(),
            JsonSerializerConfig::default().build().into(),
        );

        let sink = AzureMonitorLogsDceSink {
            service: svc,
            encoder: (self.encoding.clone(), encoder),
            batcher_settings: self.batch.into_batcher_settings()?,
        };
        Ok(VectorSink::from_event_streamsink(sink))
    }

    fn create_endpoint(&self) -> String {
        format!(
            "https://{}/dataCollectionRules/{}/streams/{}?api-version=2021-11-01-preview",
            self.endpoint_host, self.immutable_id, self.stream_name,
        )
    }
}

struct AzureMonitorLogsDceSink<Svc> {
    service: Svc,
    encoder: AzureMonitorLogsDceEncoder,
    batcher_settings: BatcherSettings,
}

impl<Svc> AzureMonitorLogsDceSink<Svc>
where
    Svc: Service<AzureMonitorLogsDceRequest> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Response: DriverResponse + Send + 'static,
    Svc::Error: Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let builder_limit = NonZeroUsize::new(64).unwrap();
        let request_builder = AzureMonitorLogsDceRequestBuilder {
            encoder: self.encoder,
        };

        input
            .batched(self.batcher_settings.as_byte_size_config())
            .request_builder(builder_limit, request_builder)
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .protocol("https")
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<Svc> StreamSink<Event> for AzureMonitorLogsDceSink<Svc>
where
    Svc: Service<AzureMonitorLogsDceRequest> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Response: DriverResponse + Send + 'static,
    Svc::Error: Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[derive(Debug)]
struct AzureMonitorLogsDceRequestPayload {
    bytes: Bytes,
}

impl From<Bytes> for AzureMonitorLogsDceRequestPayload {
    fn from(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

impl AsRef<[u8]> for AzureMonitorLogsDceRequestPayload {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

struct AzureMonitorLogsDceRequestBuilder {
    encoder: AzureMonitorLogsDceEncoder,
}

impl RequestBuilder<Vec<Event>> for AzureMonitorLogsDceRequestBuilder {
    type Metadata = EventFinalizers;
    type Events = Vec<Event>;
    type Encoder = AzureMonitorLogsDceEncoder;
    type Payload = AzureMonitorLogsDceRequestPayload;
    type Request = AzureMonitorLogsDceRequest;
    type Error = std::io::Error;

    fn compression(&self) -> Compression {
        Compression::None
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        mut events: Vec<Event>,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let finalizers = events.take_finalizers();

        let builder = RequestMetadataBuilder::from_events(&events);
        (finalizers, builder, events)
    }

    fn build_request(
        &self,
        finalizers: Self::Metadata,
        metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        AzureMonitorLogsDceRequest {
            body: payload.into_payload().bytes,
            finalizers,
            metadata,
        }
    }
}

type AzureMonitorLogsDceEncoder = (Transformer, Encoder<Framer>);

#[derive(Clone)]
pub struct AzureMonitorLogsDceRetryLogic;

// This is a clone of HttpRetryLogic for the Body type, should get merged
impl RetryLogic for AzureMonitorLogsDceRetryLogic {
    type Error = hyper::Error;
    type Response = AzureMonitorLogsDceResponse;

    fn is_retriable_error(&self, _error: &Self::Error) -> bool {
        true
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction {
        let status = response.inner.status();

        match status {
            StatusCode::TOO_MANY_REQUESTS => RetryAction::Retry("too many requests".into()),
            StatusCode::NOT_IMPLEMENTED => {
                RetryAction::DontRetry("endpoint not implemented".into())
            }
            _ if status.is_server_error() => RetryAction::Retry(status.to_string().into()),
            _ if status.is_success() => RetryAction::Successful,
            _ => RetryAction::DontRetry(format!("response status: {}", status).into()),
        }
    }
}
