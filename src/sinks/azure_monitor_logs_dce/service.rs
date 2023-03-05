use std::task::Poll;

use bytes::Bytes;
use futures::future::BoxFuture;
use http::{header::HeaderValue, Request, StatusCode};
use hyper::Body;
use snafu::Snafu;
use tower::Service;
use vector_common::request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata};

use super::auth::AzureAuthenticator;
use crate::{
    event::{EventFinalizers, EventStatus, Finalizable},
    http::HttpClient,
    sinks::prelude::DriverResponse,
};

#[derive(Debug)]
pub struct AzureMonitorLogsDceResponse {
    pub inner: http::Response<Body>,
    pub events_byte_size: GroupedCountByteSize,
    pub raw_byte_size: usize,
}

impl DriverResponse for AzureMonitorLogsDceResponse {
    fn event_status(&self) -> EventStatus {
        if self.inner.status().is_success() {
            EventStatus::Delivered
        } else if self.inner.status().is_server_error() {
            EventStatus::Errored
        } else {
            EventStatus::Rejected
        }
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.raw_byte_size)
    }
}

#[derive(Clone)]
pub(crate) struct AzureMonitorLogsDceService {
    client: HttpClient,
    uri: String,
    creds: AzureAuthenticator,
}

impl AzureMonitorLogsDceService {
    pub const fn new(client: HttpClient, uri: String, creds: AzureAuthenticator) -> Self {
        Self { client, uri, creds }
    }
}

#[derive(Debug, Snafu)]
pub enum AzureMonitorLogsDceResponseError {
    #[snafu(display("Server responded with an error: {}", code))]
    ServerError { code: StatusCode },
    #[snafu(display("Failed to make HTTP(S) request: {}", error))]
    HttpError { error: crate::http::HttpError },
}

impl Service<AzureMonitorLogsDceRequest> for AzureMonitorLogsDceService {
    type Response = AzureMonitorLogsDceResponse;
    type Error = AzureMonitorLogsDceResponseError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: AzureMonitorLogsDceRequest) -> Self::Future {
        let mut builder = Request::post(&self.uri);
        let headers = builder.headers_mut().unwrap();
        headers.insert(
            "content-type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        headers.insert(
            "content-length",
            HeaderValue::from_str(&request.body.len().to_string()).unwrap(),
        );

        let mut http_request = builder.body(Body::from(request.body)).unwrap();
        self.creds.apply(&mut http_request);

        let mut client = self.client.clone();
        Box::pin(async move {
            match client.call(http_request).await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        Ok(AzureMonitorLogsDceResponse {
                            inner: response,
                            raw_byte_size: request.metadata.request_encoded_size(),
                            events_byte_size: request
                                .metadata
                                .into_events_estimated_json_encoded_byte_size(),
                        })
                    } else {
                        Err(AzureMonitorLogsDceResponseError::ServerError { code: status })
                    }
                }
                Err(error) => Err(AzureMonitorLogsDceResponseError::HttpError { error }),
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct AzureMonitorLogsDceRequest {
    pub body: Bytes,
    pub finalizers: EventFinalizers,
    pub metadata: RequestMetadata,
}

impl Finalizable for AzureMonitorLogsDceRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for AzureMonitorLogsDceRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}
