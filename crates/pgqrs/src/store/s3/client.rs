use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;

use crate::error::{Error, Result};

/// Shared AWS S3 client builder options.
#[derive(Debug, Clone)]
pub struct AwsS3ClientConfig {
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub force_path_style: bool,
    pub credentials_provider_name: &'static str,
}

/// Build an AWS S3 SDK client from normalized configuration.
pub async fn build_aws_s3_client(config: AwsS3ClientConfig) -> aws_sdk_s3::Client {
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config.region));
    if let (Some(ak), Some(sk)) = (config.access_key, config.secret_key) {
        loader = loader.credentials_provider(Credentials::new(
            ak,
            sk,
            None,
            None,
            config.credentials_provider_name,
        ));
    }
    if let Some(ep) = config.endpoint.clone().filter(|v| !v.trim().is_empty()) {
        loader = loader.endpoint_url(ep);
    }
    let conf = loader.load().await;

    let mut s3_builder = aws_sdk_s3::config::Builder::from(&conf);
    if config.force_path_style {
        s3_builder = s3_builder.force_path_style(true);
    }
    aws_sdk_s3::Client::from_conf(s3_builder.build())
}

/// Object payload and associated ETag/revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectData {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
}

/// AWS S3-backed object store client.
#[derive(Clone, Debug)]
pub struct AwsS3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl AwsS3ObjectStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn get_object(&self, key: &str) -> Result<ObjectData> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.trim().eq_ignore_ascii_case("service error") {
                    return Error::NotFound {
                        entity: "object".to_string(),
                        id: key.to_string(),
                    };
                }
                map_s3_error("get_object", key, &msg)
            })?;

        let etag = output.e_tag().map(|s| s.to_string());

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| map_s3_error("get_object_body", key, &e.to_string()))?
            .into_bytes()
            .to_vec();

        Ok(ObjectData { bytes, etag })
    }

    pub async fn put_object_if_match(
        &self,
        key: &str,
        bytes: &[u8],
        expected_etag: Option<&str>,
    ) -> Result<String> {
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()));

        if let Some(etag) = expected_etag {
            req = req.if_match(etag.to_string());
        }

        let output = req
            .send()
            .await
            .map_err(|e| map_s3_error("put_object_if_match", key, &e.to_string()))?;

        output
            .e_tag()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::Internal {
                message: format!(
                    "S3 put_object_if_match succeeded but no ETag returned for key '{}'",
                    key
                ),
            })
    }
}

fn map_s3_error(operation: &str, key: &str, msg: &str) -> Error {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("nosuchkey")
        || lower.contains("notfound")
        || lower.contains("status code: 404")
        || lower.contains("statuscode: 404")
    {
        return Error::NotFound {
            entity: "object".to_string(),
            id: key.to_string(),
        };
    }
    if lower.contains("preconditionfailed")
        || lower.contains("if-match")
        || lower.contains("condition")
        || lower.contains("412")
    {
        return Error::Conflict {
            message: format!(
                "S3 CAS conflict during {} for key '{}': {}",
                operation, key, msg
            ),
        };
    }
    if lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("dispatch failure")
        || lower.contains("connection refused")
        || lower.contains("connection reset")
    {
        return Error::Timeout {
            operation: format!("s3:{} key={}", operation, key),
        };
    }
    Error::Internal {
        message: format!("S3 {} failed for key '{}': {}", operation, key, msg),
    }
}

#[cfg(test)]
mod tests {
    use super::map_s3_error;
    use crate::error::Error;

    #[test]
    fn map_s3_error_conflict() {
        let err = map_s3_error(
            "put_object_if_match",
            "queue.sqlite",
            "PreconditionFailed: At least one of the pre-conditions you specified did not hold",
        );
        assert!(matches!(err, Error::Conflict { .. }));
    }

    #[test]
    fn map_s3_error_timeout() {
        let err = map_s3_error(
            "get_object",
            "queue.sqlite",
            "dispatch failure: operation timed out",
        );
        assert!(matches!(err, Error::Timeout { .. }));
    }

    #[test]
    fn map_s3_error_not_found() {
        let err = map_s3_error(
            "get_object",
            "queue.sqlite",
            "NoSuchKey: The specified key does not exist",
        );
        assert!(matches!(err, Error::NotFound { .. }));
    }

    #[test]
    fn map_s3_error_fallback_internal() {
        let err = map_s3_error("get_object", "queue.sqlite", "some random provider error");
        assert!(matches!(err, Error::Internal { .. }));
    }
}
