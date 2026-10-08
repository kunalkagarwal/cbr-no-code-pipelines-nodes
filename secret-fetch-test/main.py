#!/usr/bin/env python3
"""
Secret Fetch Test Node - Custom Node Container

Proves the Secret Manager (docs/SECRET_MANAGER.md in cbr-no-code-editor)
works from inside a REAL running custom node pod, not a script pretending
to be one. Calls env_sdk.get_secret(secretName) at run time and writes a
proof file: whether the fetch succeeded, the value's length, and a
SHA-256 hash of the value. The real secret value is never written to any
output file, and is not printed to logs -- except when the "printSecretValue"
demo switch is on, which exists to show log masking: with masking on (the
default) the SDK replaces the value with [REDACTED] in the logs; with the
"disableLogMasking" switch on, the raw value appears in plain text (the risk
masking protects against).

Follows the NodeContext contract - receives a single NODE_CONTEXT JSON,
same as every other node in this repo (see hello-csv-source/main.py).

Resource Environment Variables used here:
  BACKEND_URL   cluster-internal backend base URL
  API_KEY       bearer credential resolved server-side to this node's owner
"""

import hashlib
import json
import os
import sys

import env_sdk
import storage_v2

NODE_VERSION = "2026-10-08.1"


def log(msg):
    print(f"[SECRET FETCH TEST] {msg}", flush=True)


def log_error(msg):
    print(f"[SECRET FETCH TEST ERROR] {msg}", file=sys.stderr, flush=True)


def parse_context():
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    return json.loads(raw)


def main():
    log(f"version {NODE_VERSION}")
    ctx = parse_context()
    node = ctx.get("node", {})

    try:
        config = ctx["config"]
        out_files = ctx["output"]["files"]
        secret_name = config["secretName"]
        print_secret = bool(config.get("printSecretValue", False))
        # Boolean fields have no default in the node contract (an untouched toggle
        # is off), so the switch is phrased "disable": off = masking on.
        mask_logs = not bool(config.get("disableLogMasking", False))

        log(f"Node: {node['name']} | Fetching secret named {secret_name!r} via env_sdk.get_secret()")
        log(f"Demo switches: printSecretValue={print_secret} logMasking={mask_logs}")

        try:
            value = env_sdk.get_secret(secret_name, mask=mask_logs)
            if print_secret:
                # Deliberate leak, for the demo only: with masking on this prints
                # [REDACTED]; with masking off the real value lands in the logs.
                log(f"DEMO: the secret value is {value}")
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
                "logMasking": mask_logs,
                "demoPrintedSecret": print_secret,
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

        # main_file["path"] is "s3://bucket/{userId}/rest/of/path" (from
        # NODE_CONTEXT) -- storage_v2.write_bytes() wants the path relative to
        # the caller's own prefix, i.e. everything after "{userId}/".
        main_file = out_files[0]
        s3_path = main_file["path"]
        rest = s3_path[len("s3://"):] if s3_path.startswith("s3://") else s3_path
        relative_path = "/".join(rest.split("/")[2:])  # drop "bucket/{userId}"
        log(f"Uploading -> {main_file['path']} via storage_v2.write_bytes()...")
        storage_v2.write_bytes(relative_path, data, content_type="application/json")

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
