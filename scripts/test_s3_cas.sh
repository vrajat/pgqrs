#!/usr/bin/env bash
set -euo pipefail

# Minimal S3 CAS probe using conditional PutObject headers.
#
# Usage examples:
#   # LocalStack
#   AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
#   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
#   ./scripts/test_s3_cas.sh --bucket pgqrs-test-bucket
#
#   # Real AWS
#   AWS_REGION=us-east-1 ./scripts/test_s3_cas.sh --bucket my-real-bucket

BUCKET=""
KEY_PREFIX="${KEY_PREFIX:-pgqrs/cas-probe}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)
      BUCKET="${2:-}"
      shift 2
      ;;
    --key-prefix)
      KEY_PREFIX="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${BUCKET}" ]]; then
  echo "Missing required --bucket" >&2
  exit 2
fi

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Expected .venv/bin/python to exist. Run project bootstrap first." >&2
  exit 2
fi

BUCKET="${BUCKET}" KEY_PREFIX="${KEY_PREFIX}" .venv/bin/python - <<'PY'
import os
import sys
import uuid

import boto3
from botocore.exceptions import ClientError

bucket = os.environ["BUCKET"]
key_prefix = os.environ["KEY_PREFIX"].rstrip("/")
key = f"{key_prefix}/{uuid.uuid4()}.txt"

session = boto3.session.Session(region_name=os.getenv("AWS_REGION", "us-east-1"))
client = session.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))

print(f"Bucket: {bucket}")
print(f"Endpoint: {os.getenv('AWS_ENDPOINT_URL', '<aws>')}")
print(f"Key: {key}")

def put(body: bytes, **kwargs):
    return client.put_object(Bucket=bucket, Key=key, Body=body, **kwargs)

def head_etag() -> str:
    return client.head_object(Bucket=bucket, Key=key)["ETag"]

def expect_client_error(fn, expected_codes):
    try:
        fn()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in expected_codes:
            print(f"Got expected error code: {code}")
            return code
        raise
    raise AssertionError("Expected ClientError, but call succeeded")

print("\n1) Creating object (unconditional put)")
put(b"v1")
etag1 = head_etag()
print(f"etag1={etag1}")

print("\n2) Updating object (unconditional put)")
put(b"v2")
etag2 = head_etag()
print(f"etag2={etag2}")

if etag1 == etag2:
    raise AssertionError("ETag did not change across overwrite; probe invalid")

print("\n3) Stale CAS write: IfMatch=etag1 (must fail if CAS enforced)")
stale_failed = False
try:
    expect_client_error(
        lambda: put(b"stale-should-fail", IfMatch=etag1),
        {"PreconditionFailed", "412"},
    )
    stale_failed = True
except Exception as e:
    print(f"Stale CAS write was NOT rejected as expected: {e}")

print("\n4) Fresh CAS write: IfMatch=etag2 (should succeed)")
put(b"fresh-should-pass", IfMatch=etag2)
etag3 = head_etag()
print(f"etag3={etag3}")

print("\n5) IfNoneMatch='*' on existing key (should fail)")
none_match_failed = False
try:
    expect_client_error(
        lambda: put(b"create-only-should-fail", IfNoneMatch="*"),
        {"PreconditionFailed", "412"},
    )
    none_match_failed = True
except Exception as e:
    print(f"IfNoneMatch='*' was NOT rejected as expected: {e}")

print("\n6) Cleanup")
client.delete_object(Bucket=bucket, Key=key)

print("\n=== CAS Probe Summary ===")
print(f"stale_if_match_rejected={stale_failed}")
print(f"if_none_match_rejected={none_match_failed}")

if not stale_failed or not none_match_failed:
    print(
        "RESULT: Backend did not enforce one or more conditional PUT preconditions.",
        file=sys.stderr,
    )
    sys.exit(1)

print("RESULT: Backend enforced conditional PUT CAS semantics.")
PY
