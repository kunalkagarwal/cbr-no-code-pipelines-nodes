#!/usr/bin/env python3
"""
Secret Fetch Test Node - Custom Node Container

Proves the Secret Manager (docs/SECRET_MANAGER.md in cbr-no-code-editor)
works from inside a REAL running custom node pod, not a script pretending
to be one. Calls env_sdk.get_secret(secretName) at run time and writes a
proof file: whether the fetch succeeded, the value's length, and a
SHA-256 hash of the value. The real secret value is NEVER printed to logs
or written to any output file.

Follows the NodeContext contract - receives a single NODE_CONTEXT JSON,
same as every other node in this repo (see hello-csv-source/main.py).

Resource Environment Variables used here:
  BACKEND_URL   cluster-internal backend base URL
  API_KEY       bearer credential resolved server-side to this node's owner
  ARTIFACT_S3_* artifact bucket credentials, for writing the output file
"""

import hashlib
import io
import json
import os
import sys

import env_sdk
import storage_v2 as boto3
import requests
from botocore.client import Config

NODE_VERSION = "2026-09-24.1"


def log(msg):
    print(f"[SECRET FETCH TEST] {msg}", flush=True)


def log_error(msg):
    print(f"[SECRET FETCH TEST ERROR] {msg}", file=sys.stderr, flush=True)


def parse_context():
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    return json.loads(raw)


def split_s3(s3_path):
    rest = s3_path[len("s3://"):] if s3_path.startswith("s3://") else s3_path
    bucket, _, key = rest.partition("/")
    return bucket, key


def get_s3_client(prefix):
    endpoint = os.environ[f"{prefix}_S3_ENDPOINT"]
    use_ssl = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
    if "://" not in endpoint:
        endpoint = ("https://" if use_ssl else "http://") + endpoint
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ[f"{prefix}_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_S3_SECRET_KEY"],
        aws_session_token=os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get(f"{prefix}_S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def main():
    log(f"version {NODE_VERSION}")
    ctx = parse_context()
    node = ctx.get("node", {})

    try:
        config = ctx["config"]
        out_files = ctx["output"]["files"]
        secret_name = config["secretName"]

        log(f"Node: {node['name']} | Fetching secret named {secret_name!r} via env_sdk.get_secret()")

        try:
            value = env_sdk.get_secret(secret_name)
            value_len = len(value)
            value_hash = hashlib.sha256(value.encode("utf-8")).hexdigest()
            log(f"Fetched OK. length={value_len} sha256={value_hash[:16]}...")
            result = {
                "success": True,
                "nodeName": node["name"],
                "secretName": secret_name,
                "fetched": True,
                "valueLength": value_len,
                "valueSha256": value_hash,
                "note": "The real secret value is never included here or in any log line.",
            }
        except env_sdk.EnvError as e:
            # A missing secret is a legitimate, informative outcome for this
            # test node -- report it in the output rather than crashing, so
            # "the name was wrong" is distinguishable from "the fetch is broken".
            log_error(f"get_secret failed: {e}")
            result = {
                "success": True,
                "nodeName": node["name"],
                "secretName": secret_name,
                "fetched": False,
                "error": str(e),
            }

        data = json.dumps(result, indent=2).encode("utf-8")
        log(f"Writing {len(data)} bytes of proof JSON")

        os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
        os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

        main_file = out_files[0]
        main_presigned_url = main_file.get("presignedUrl")
        if main_presigned_url:
            log(f"Uploading -> {main_file['path']} via presigned URL...")
            resp = requests.put(main_presigned_url, data=data, timeout=300)
            resp.raise_for_status()
        else:
            artifact_s3 = get_s3_client("ARTIFACT")
            dest_bucket, dest_key = split_s3(main_file["path"])
            log(f"Uploading -> s3://{dest_bucket}/{dest_key} ...")
            artifact_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=data)

        log(f"Completed OK (version {NODE_VERSION})")
        print(json.dumps({
            "success": True,
            "version": NODE_VERSION,
            "nodeName": node["name"],
            "secretFetched": result["fetched"],
            "outputs": [{"name": f["name"], "path": f["path"]} for f in out_files],
        }))

    except Exception as e:
        log_error(str(e))
        print(json.dumps({"success": False, "nodeName": node.get("name"), "error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
