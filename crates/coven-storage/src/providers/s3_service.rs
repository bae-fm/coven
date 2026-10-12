use url::Url;

/// Endpoint identity selects protocol behavior; unknown services must pass setup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum S3Service {
    Aws,
    Gcs,
    Compatible,
}

impl S3Service {
    pub(super) fn for_endpoint(endpoint: Option<&Url>) -> Self {
        match endpoint {
            None => Self::Aws,
            Some(endpoint) if endpoint.host_str() == Some("storage.googleapis.com") => Self::Gcs,
            Some(_) => Self::Compatible,
        }
    }
}
