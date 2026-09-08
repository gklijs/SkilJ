//! Real-network-free tests for `access_control::verify_and_extract_subject`,
//! using a real RSA keypair, a real local `axum` server serving its JWKS,
//! and real JWTs signed with `jsonwebtoken::encode` (docs/architecture.md
//! [§6](../../docs/architecture.md#idp-trust-configuration)). Genuine I/O (unlike this crate's own pure-function tests), so a
//! dedicated harness rather than `#[test] fn ... { pure_call() }` - a
//! real server, not a mock, since `verify_and_extract_subject` genuinely
//! fetches the JWKS document over HTTP via `reqwest`.
//!
//! The RSA keypair below was generated once, locally, purely to serve as
//! a fixed test fixture - `openssl genrsa 2048` plus a small script
//! computing the matching JWK `n`/`e` from the public modulus/exponent.
//! Never used for anything but these tests.

use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use skilj_core::access_control::{
    verify_and_extract_subject, IdpConfig, JwksCache, SigningAlgorithm,
};
use skilj_core::error::SkiljRejection;

const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPHVFsUHiWXSbG
/TCig1cTQHNT6FnoYoZtMEjvDiQArsOL/dFoM9pmGRM9CfEtQGNum4TsimPtgJec
awfdPnW0uJCRlIF9wGmYdh2mYNBKw8jqxwp664Gd5uqH5L6A4pN8bfGO7+2niD6p
8t0cNeyYOd0PusbAEDcpzCUZmr6KQyM5i8/wk5oO98gntp+ZpMjUZabAD6R8DyhM
IZmV645jo5NPJG7zuSz+3dmKkNY0/GXz8YwvZ2swqmmOANRZHHfN1vgP2ycK02WZ
4yihx6EiuQCDseddBw+xit9KSvSq6GwmwnV1qVpMVNlSGGOeVX7v7JQ3z/BNbQ85
5p6s/FjhAgMBAAECggEAFu8fKghLIhNUjOpSbVxv0vDrFFqBQitOyV50ZQxCzlSL
0L+dZZWAVJfoOnUUYLdli0TrVioI4K7Bmw97AnO9IvLhB03TfPJGfxxtMhQ8XFsL
r3u03GGhq7N7OusIcUslm7ys5/AHd+qtTbJX65zJAx49LVW4VmI1SYqSfSBWgway
8uGYaXyCfwuxQ+xB4fQd6llm/+9dqS+U36LVSMWgEmVjceorYFhPVLfuX4A1wHjF
mDl40AwPBqzVbOIzFDMDikk4heFi6wlt6N3LGDtyBUUuzEg5TBhyiirvNvTjW+4V
Z4MZs3tez+IqM0+F4EsgAEQUU12YQxa4lobm8/zgZQKBgQD81FMzymNR6xWhUSwY
4RtkVntfMBOMp1rVGcVyBxOLKxEXF6ctk2rV38krfUI50h/lWzrbpl+zJvEe8D1H
vZjYj28sL3wf0CSnPYUeGANTxrW1dTiz1HVzzChfbAEWj3fsVrlghNcnHBkDDhqz
L/rPEfp//fB0SyLAEAJt87cgFwKBgQDRtjtH1gIkGn5GCS3u0FAbxV+qrUlTvu4t
Di1GcEw32jootQQSMZN1PxEvLuehaBlaASEL2OZzZlQ4q60LV1Jisvd7wqv5EYnG
o+sKtrCS5iXKfkxqTmg+JS7OZazggyvgBnv4GXT0US6/G4nw7C9JaS2jyOvPGIPS
K8dsWDIxxwKBgQCgr4FBxTticPqKUECqf0cdeilm0fNazXJZRcvLMNwm8vQlrQ6/
VJXt4BDG5xEUFovXBShfOVpRTkqo0x7fXYyq9l49wuAsh+kDsYHNIo3azMvny9yB
zmHnerWeD9KROBWLy4J96W+kl6L94hTuFWxd9psyhX4xKx+m2YXxw5d7eQKBgFB2
I86PHOkvRQ2oDfiX8nSFSQxaSk0Yb5fX3aUuBwBS+YeO1E4KuXH9zaEV1QeHwlpX
Ho/GG71hIKVRsSYtzc1Sr0PL0GHSydLuJ4tHxv3F0fAcf0M2bCaT656DQk4t5dKh
ikUJt2baEx59+XH3nLkE4t75gwhFdqZX5775I+EXAoGAfnpHlLZdGW48rl9Cl887
hRDjXDm/gP/ljCrvxxiWselEgaLj2o4NiT28QAfq7KgtOIpAeLAGzIBP6vkE7KFp
nAF+t4gRpooXXSI5oXCBcGI9a26q68UV3iDEmQGiP8kVHOsdzcOKY0qk1ulNAIV4
fU919gnTKorSq3FdV6zGZ8s=
-----END PRIVATE KEY-----
";

/// A second, entirely different keypair - only its private half is used,
/// to sign a JWT the server's own published JWKS (built from
/// `TEST_PRIVATE_KEY_PEM`'s public half) can never verify.
const OTHER_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC6IZrUe8JMpf2I
I+vgQgvXaUC7Xv17Q0iv+nZt32D5RGW4cQ36yvk+9eUV4tNdWwW2/XWECcBFdcVW
UzNkLBiRQzjdL/tvL5xglq1pVeFDPwJKFqH/0qKhBwUNWhNtern/jeQGAuPrg41C
gIc118gAEoNlfD0NyhAHeJ96hYX4xxrTFFKKCbxzE6FJho7B38I+qfe5WDUwVCq3
deNks41Hyq33HsKZFNQwyM/zZ5VfI2NF8J1PBGadCxp1z19ecErkQU+tRM4NDLPm
xl8EikhZzHkNpMafleUsb0kvuesiG235m43NjX2ZTPLI7R42fFtzt6hEx2wjGxM1
xzTCC6XjAgMBAAECggEAGb4cE9cqAD/U2MdfEB0SVjCQa1mv5SRhITWau4dxeggj
qWa5cD4ySqrnjCda5EZ6e9yCLEjM9s5bBJ1tNiWDIFQTwUOpmq8TCajNQwxFo29L
ecO3lBIlu++kmzwiA7o9j0KojsxHiYMgPlpYWPIHDzuAQMD2ELopjV75b5CX/tgN
6hNQ0JBafJoyP+g+eIfBUdEKVDTqGMLq00y8nSg2FXUNlWtCd+ACjT6+2rXuTGiE
Q7MXUkHQBPJd2fXlfqIbn8zb6nN9U9UNOkvXq2mXXsk0+pwqMmUJB3ZR7dvIVXl6
Ni5nNxYULrtWpneuivr/i4ICQzmmECIsDfoWvjCZQQKBgQD1aHWyWdXHRQxc+yDw
M8z9oDdfCPpwxukQFicIqMlJSyEkPC32CiwQyuGyagro257GuA0iCZziCf7Bi+nY
sRL2+RS8fHmvzLQ1RaPZLVIOyKqZeh4HAFvvQEpYTBGwlWUAswSnOj4mMtwAUZG5
I7CHs2ZiM5EOU0Wx8J783gzNfwKBgQDCKjCBzTRSzv9kGwlAidSUcowC9m6JDYqi
s59LbdOUQ79egRLrppBZR5bJqx6Ca9OstBeXI0EK0rLK5UWR2MIJhxCtpWZZ68YM
xQfOKlVLLiaDDi8rqouoS4NZKNyzK7q6j1PeD26n7ZJ2JLT0eckLYHim2fhri11I
aVyBUOvhnQKBgFAQDV3raxBA2aC4GW3kKHuSOp5ZqoMCkeS6pW9wyYKM7ToKHCCJ
/whXeDyh1f9ULz+7qiUxp6ojAqcYQ2l7k6lZZ8d6gKS3Dw/WMXdYDs5d7zJ1Ibi9
CEFM4zRdVOQcSUBqJxl7qe0CaL393qHdH+mVwNBG7IsU5ccAro3mz5x3AoGBAInz
dcFTbaCEJ5oVR26OPvY1qFqWghRoBZ7xpfTulAvcUoQviqTZE+gK4AxqwuOA/sTO
s5ATYSvyZUuYt+QWsE4ao3PsdxreVDlQZ+pH04/1uzEUC9mnc1BgTnMzgBLgt+vC
16CHMGSpe4zrKZIlUPz/TtmlNkYan21KRoouV1lVAoGBAKLBmfRdI5qGP7M+68pp
P+D7xgRaEhjv4pUIHj4lDEdB2COtPfrv9cgMcOMhxeBtipkKjU94eFuUuKgqfqkw
4i7u1+idsqCphvQoYCTnwydZi8sC7xDYhg/mr9jbWuG944rwwlkJ5avkpKYKOY7t
UWJLfBV4jkvwPqQDX4FXKl2G
-----END PRIVATE KEY-----
";

const TEST_MODULUS_N: &str = "zx1RbFB4ll0mxv0wooNXE0BzU-hZ6GKGbTBI7w4kAK7Di_3RaDPaZhkTPQnxLUBjbpuE7Ipj7YCXnGsH3T51tLiQkZSBfcBpmHYdpmDQSsPI6scKeuuBnebqh-S-gOKTfG3xju_tp4g-qfLdHDXsmDndD7rGwBA3KcwlGZq-ikMjOYvP8JOaDvfIJ7afmaTI1GWmwA-kfA8oTCGZleuOY6OTTyRu87ks_t3ZipDWNPxl8_GML2drMKppjgDUWRx3zdb4D9snCtNlmeMoocehIrkAg7HnXQcPsYrfSkr0quhsJsJ1dalaTFTZUhhjnlV-7-yUN8_wTW0POeaerPxY4Q";
const TEST_EXPONENT_E: &str = "AQAB";
const TEST_KID: &str = "test-key-1";
const TEST_ISSUER: &str = "https://idp.example.test/";

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for jwt tests")
    })
}

/// Spins up a real local HTTP server serving one JWKS document (built
/// from `TEST_MODULUS_N`/`TEST_EXPONENT_E` under `TEST_KID`) on an
/// ephemeral port, and returns its `/jwks.json` URL. The spawned task
/// outlives the test (never explicitly stopped) - each test binds its
/// own fresh port, so nothing collides.
async fn serve_jwks() -> String {
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": TEST_KID,
            "n": TEST_MODULUS_N,
            "e": TEST_EXPONENT_E,
        }]
    });
    let app = axum::Router::new().route(
        "/jwks.json",
        axum::routing::get(move || {
            let jwks = jwks.clone();
            async move { axum::Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind an ephemeral port for the JWKS test server");
    let addr = listener
        .local_addr()
        .expect("a bound listener always has a local address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the JWKS test server stopped unexpectedly");
    });
    format!("http://{addr}/jwks.json")
}

fn idp_config(jwks_url: &str) -> IdpConfig {
    IdpConfig::new(
        jwks_url
            .parse()
            .expect("the test server's own URL is well-formed"),
        TEST_ISSUER,
        SigningAlgorithm::Rs256,
    )
}

/// Signs a JWT with `TEST_PRIVATE_KEY_PEM` (unless `signing_key_pem`
/// says otherwise - see `verify_and_extract_subject_rejects_a_bad_signature`),
/// `exp` one hour out unless `expires_in_the_past` - real signing, real
/// expiry, exercising `jsonwebtoken::decode`'s own validation for real
/// rather than mocking it away.
fn sign_jwt(
    subject_claim: &str,
    subject_value: &str,
    kid: &str,
    issuer: &str,
    signing_key_pem: &str,
    expires_in_the_past: bool,
) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let exp = if expires_in_the_past {
        chrono::Utc::now() - chrono::Duration::hours(1)
    } else {
        chrono::Utc::now() + chrono::Duration::hours(1)
    };
    let claims = json!({
        subject_claim: subject_value,
        "iss": issuer,
        "exp": exp.timestamp(),
    });
    let key = EncodingKey::from_rsa_pem(signing_key_pem.as_bytes())
        .expect("the test private key PEM is well-formed");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing a well-formed JWT never fails")
}

#[test]
fn verify_and_extract_subject_succeeds_for_a_validly_signed_jwt() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let config = idp_config(&jwks_url);
        let cache = JwksCache::new(config.jwks_endpoint.clone());
        let jwt = sign_jwt(
            "sub",
            "user-123",
            TEST_KID,
            TEST_ISSUER,
            TEST_PRIVATE_KEY_PEM,
            false,
        );

        let subject = verify_and_extract_subject(&jwt, &config, &cache)
            .await
            .unwrap();
        assert_eq!(subject, "user-123");
    });
}

#[test]
fn verify_and_extract_subject_respects_a_configured_subject_claim() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let config = idp_config(&jwks_url).with_subject_claim("email");
        let cache = JwksCache::new(config.jwks_endpoint.clone());
        let jwt = sign_jwt(
            "email",
            "user@example.com",
            TEST_KID,
            TEST_ISSUER,
            TEST_PRIVATE_KEY_PEM,
            false,
        );

        let subject = verify_and_extract_subject(&jwt, &config, &cache)
            .await
            .unwrap();
        assert_eq!(subject, "user@example.com");
    });
}

#[test]
fn verify_and_extract_subject_rejects_a_bad_signature() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let config = idp_config(&jwks_url);
        let cache = JwksCache::new(config.jwks_endpoint.clone());
        // Signed with a different key than the one the server's JWKS
        // publishes under this kid - the signature can never verify.
        let jwt = sign_jwt(
            "sub",
            "user-123",
            TEST_KID,
            TEST_ISSUER,
            OTHER_PRIVATE_KEY_PEM,
            false,
        );

        let err = verify_and_extract_subject(&jwt, &config, &cache)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "jwt_verification_failed");
    });
}

#[test]
fn verify_and_extract_subject_rejects_an_expired_jwt() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let config = idp_config(&jwks_url);
        let cache = JwksCache::new(config.jwks_endpoint.clone());
        let jwt = sign_jwt(
            "sub",
            "user-123",
            TEST_KID,
            TEST_ISSUER,
            TEST_PRIVATE_KEY_PEM,
            true,
        );

        let err = verify_and_extract_subject(&jwt, &config, &cache)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "jwt_verification_failed");
    });
}

#[test]
fn verify_and_extract_subject_rejects_a_wrong_issuer() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let config = idp_config(&jwks_url);
        let cache = JwksCache::new(config.jwks_endpoint.clone());
        let jwt = sign_jwt(
            "sub",
            "user-123",
            TEST_KID,
            "https://a-different-idp.example.test/",
            TEST_PRIVATE_KEY_PEM,
            false,
        );

        let err = verify_and_extract_subject(&jwt, &config, &cache)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "jwt_verification_failed");
    });
}

/// The reactive-refresh path ([docs/architecture.md §6](../../docs/architecture.md#idp-trust-configuration)): a `kid` the
/// cache has never seen triggers exactly one JWKS refetch before giving
/// up - still `None` afterward here, since the test server never
/// publishes a key under this `kid` at all.
#[test]
fn verify_and_extract_subject_rejects_an_unknown_kid_even_after_refetch() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let config = idp_config(&jwks_url);
        let cache = JwksCache::new(config.jwks_endpoint.clone());
        let jwt = sign_jwt(
            "sub",
            "user-123",
            "no-such-kid",
            TEST_ISSUER,
            TEST_PRIVATE_KEY_PEM,
            false,
        );

        let err = verify_and_extract_subject(&jwt, &config, &cache)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "unknown_signing_key");
    });
}
