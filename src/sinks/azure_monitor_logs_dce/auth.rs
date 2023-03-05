use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use azure_core::auth::{AccessToken, TokenCredential};
use azure_identity::{ClientSecretCredential, DefaultAzureCredential, TokenCredentialOptions};
use http::header::AUTHORIZATION;
use tokio::{
    sync::watch::{self},
    time::Instant,
};

use super::sink::AzureClientSecretCredentials;

struct Inner {
    credential: Arc<dyn TokenCredential>,
    token: RwLock<AccessToken>,
}

impl Inner {
    fn get_token(&self) -> String {
        self.token.read().unwrap().secret().to_string()
    }

    async fn regenerate_token(&self) -> crate::Result<()> {
        let token = fetch_token(self.credential.clone()).await?;
        *self.token.write().unwrap() = token;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct AzureAuthenticator {
    inner: Arc<Inner>,
}

impl AzureAuthenticator {
    pub async fn new(
        client_credentials: Option<&AzureClientSecretCredentials>,
    ) -> crate::Result<Self> {
        let credential: Arc<dyn TokenCredential> = if let Some(creds) = client_credentials {
            Arc::new(ClientSecretCredential::new(
                // XXX use vector's http client
                azure_core::new_http_client(),
                creds.tenant_id.clone(),
                creds.client_id.clone(),
                creds.client_secret.clone(),
                TokenCredentialOptions::default(),
            ))
        } else {
            Arc::new(DefaultAzureCredential::default())
        };
        let token = fetch_token(credential.clone()).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                credential,
                token: RwLock::new(token),
            }),
        })
    }

    pub fn apply<T>(&self, request: &mut http::Request<T>) {
        let token = self.inner.get_token();
        request
            .headers_mut()
            .insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());
    }

    pub fn spawn_regenerate_token(&self) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        tokio::spawn(self.clone().token_regenerator(sender));
        receiver
    }

    async fn token_regenerator(self, sender: watch::Sender<()>) {
        let period = Duration::from_secs(60 * 60);
        let mut interval = tokio::time::interval_at(Instant::now() + period, period);
        loop {
            interval.tick().await;
            debug!("Renewing Azure authentication token.");
            match self.inner.regenerate_token().await {
                Ok(()) => sender.send_replace(()),
                Err(error) => {
                    error!(
                        message = "Failed to update Azure authentication token.", %error
                    )
                }
            }
        }
    }
}

async fn fetch_token(credential: Arc<dyn TokenCredential>) -> crate::Result<AccessToken> {
    let response = credential.get_token("https://monitor.azure.com/").await?;
    Ok(response.token)
}
