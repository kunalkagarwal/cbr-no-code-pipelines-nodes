"""Secret Check Source (Storage v2).

A source node (no upstream input) that fetches one of the caller's own stored
secrets by name, to prove the secret manager works inside a real running node.
The value is never written to the output file.
"""

import hashlib
import json
import os

from pipeline_sdk import context, env
from pipeline_sdk.context import log

NODE_VERSION = "2026-10-21.1"


def handler(ctx: context.NodeContext, workdir: str):
    log(f"Secret check source, version {NODE_VERSION}")
    name = str(ctx.config.get("secretName") or "").strip()
    if not name:
        raise ValueError("secretName is required")

    log(f"Fetching the secret named '{name}' via env.get_secret()")
    value = env.get_secret(name)  # raises EnvError (no value in its message) when missing
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    log(f"Fetched OK. length={len(value)} sha256={digest[:12]}...")

    # On purpose: the SDK registered the value for log masking, so this line
    # must show [REDACTED] in the run's logs, never the real text.
    print(f"Secret value (must show as [REDACTED] in the log): {value}", flush=True)

    proof = {"fetched": True, "secretName": name, "length": len(value), "sha256Prefix": digest[:12]}
    result_path = os.path.join(workdir, "result.json")
    with open(result_path, "w", encoding="utf-8") as handle:
        json.dump(proof, handle)
    for out in ctx.outputs:
        log(f"Uploading proof to {out.path}")
        out.upload_file(result_path)
    return {"fetched": True, "length": len(value)}


if __name__ == "__main__":
    context.run(handler, prefix="[SECRET CHECK]")
