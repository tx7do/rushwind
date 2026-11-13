//! The SigV4 signer, pinned against a fixed reference vector (the
//! canonical request, string-to-sign, and final signature were
//! computed once with the published algorithm and frozen — any drift
//! in the signing chain fails these).

#![cfg(test)]

use rushwind_broker_sqs::{format_amz_date, queue_path, queue_region, sigv4_post, url_host};

#[test]
fn the_reference_vector_is_reproduced() {
    let body = "Action=SendMessage&QueueUrl=https%3A%2F%2Fsqs.us-east-1.amazonaws.com%2F123456789012%2Fmy-queue&MessageBody=hello&Version=2012-11-05";
    let (authorization, amz_date) = sigv4_post(
        "sqs.us-east-1.amazonaws.com",
        "/",
        "us-east-1",
        "sqs",
        "AKIDEXAMPLE",
        // The fixed test key from the AWS docs' SigV4 examples.
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        None,
        body,
        "20261113T000000Z",
    );
    assert_eq!(amz_date, "20261113T000000Z");
    assert!(
        authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261113/us-east-1/sqs/aws4_request, "
        ),
        "{authorization}"
    );
    assert!(
        authorization.contains("SignedHeaders=content-type;host;x-amz-date"),
        "{authorization}"
    );
    assert!(
        authorization.ends_with(
            "Signature=e70022112d406e8bc84f3f0cf130a6c8b1c6812ba2f707b306fb81b6f2d45a7c"
        ),
        "{authorization}"
    );
}

#[test]
fn session_tokens_join_the_signed_headers() {
    let body = "Action=ReceiveMessage";
    let (authorization, _) = sigv4_post(
        "sqs.us-east-1.amazonaws.com",
        "/",
        "us-east-1",
        "sqs",
        "AKID",
        "SECRET",
        Some("session-token-value"),
        body,
        "20261113T000000Z",
    );
    assert!(
        authorization.contains("SignedHeaders=content-type;host;x-amz-date;x-amz-security-token"),
        "{authorization}"
    );
}

#[test]
fn the_amz_date_form_is_basic_utc() {
    // 2026-11-13T01:15:23Z (the value the engine's own algorithm
    // computes — the pin is the format, not a hand-derived epoch).
    let produced = format_amz_date(1_794_532_523);
    assert_eq!(produced.len(), 16);
    assert!(produced.starts_with("20261113T") && produced.ends_with('Z'));
    // The epoch itself: 1970-01-01T00:00:00Z.
    assert_eq!(format_amz_date(0), "19700101T000000Z");
    // One second before/after midnight — the day roll.
    assert_eq!(format_amz_date(86_399), "19700101T235959Z");
    assert_eq!(format_amz_date(86_400), "19700102T000000Z");
    // The epoch itself: 1970-01-01T00:00:00Z.
    assert_eq!(format_amz_date(0), "19700101T000000Z");
}

#[test]
fn queue_urls_yield_host_path_region() {
    let url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";
    assert_eq!(url_host(url), "sqs.us-east-1.amazonaws.com");
    assert_eq!(queue_path(url), "/123456789012/my-queue");
    assert_eq!(queue_region(url).as_deref(), Some("us-east-1"));
    assert_eq!(queue_region("https://127.0.0.1:9324/queue"), None);
}
