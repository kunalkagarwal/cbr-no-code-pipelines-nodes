"""GeoJSON URL Source (Storage v2).

A source node (no upstream input) that downloads one file from a web address
and writes it unchanged as the node output, so the editor's map view can be
tried with real GeoJSON files from the internet.
"""

import json
import os
import urllib.error
import urllib.request

from pipeline_sdk import context
from pipeline_sdk.context import log

NODE_VERSION = "2026-10-16.1"
MAX_BYTES = 100 * 1024 * 1024
GEOJSON_TYPES = {
    "FeatureCollection", "Feature", "Point", "MultiPoint", "LineString",
    "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection",
}


def validate_url(raw) -> str:
    url = str(raw or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        raise ValueError("url must start with http:// or https://")
    return url


def download(url: str, dest: str) -> int:
    """Stream `url` to `dest`; fail if it is larger than MAX_BYTES. Returns the size."""
    request = urllib.request.Request(url, headers={"User-Agent": "cbr-geojson-url-source/1.0"})
    size = 0
    with urllib.request.urlopen(request, timeout=60) as response, open(dest, "wb") as out:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_BYTES:
                raise ValueError(f"file is larger than {MAX_BYTES // (1024 * 1024)} MB")
            out.write(chunk)
    return size


def describe(path: str) -> str:
    """Parse the downloaded file as JSON; say what it is. Raises if it is not JSON."""
    with open(path, "rb") as handle:
        data = json.load(handle)
    kind = data.get("type") if isinstance(data, dict) else None
    if kind not in GEOJSON_TYPES:
        return "valid JSON but NOT GeoJSON (the map will refuse it)"
    count = len(data.get("features", [])) if kind == "FeatureCollection" else 1
    return f"GeoJSON {kind} with {count} feature(s)/geometry"


def handler(ctx: context.NodeContext, workdir: str):
    log(f"GeoJSON URL source, version {NODE_VERSION}")
    url = validate_url(ctx.config.get("url"))
    log(f"Downloading {url}")
    result_path = os.path.join(workdir, "result.geojson")
    try:
        size = download(url, result_path)
    except urllib.error.HTTPError as error:
        raise ValueError(f"the server answered HTTP {error.code} for that address") from error
    except urllib.error.URLError as error:
        raise ValueError(f"could not reach that address: {error.reason}") from error
    log(f"Downloaded {size} bytes")
    try:
        summary = describe(result_path)
    except ValueError as error:  # json.JSONDecodeError is a ValueError
        raise ValueError(f"the file is not valid JSON: {error}") from error
    log(f"File check: {summary}")

    for out in ctx.outputs:
        log(f"Uploading to {out.path}")
        out.upload_file(result_path)

    return {"bytes": size, "summary": summary}


if __name__ == "__main__":
    context.run(handler, prefix="[GEOJSON URL]")
