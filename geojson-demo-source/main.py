"""GeoJSON Demo Source (Storage v2).

A source node (no upstream input) that writes one GeoJSON FeatureCollection of
sample Bengaluru features, so the map view can be tried on a real run.
"""

import json
import os

from pipeline_sdk import context
from pipeline_sdk.context import log

NODE_VERSION = "2026-10-16.1"


def feature(name: str, kind: str, geometry: dict) -> dict:
    return {"type": "Feature", "properties": {"name": name, "kind": kind}, "geometry": geometry}


def build_feature_collection() -> dict:
    """Landmark points, two park polygons and a road line ([lon, lat] order)."""
    points = [
        ("Vidhana Soudha", [77.5907, 12.9796]),
        ("Lalbagh Botanical Garden (gate)", [77.5848, 12.9507]),
        ("Cubbon Park (bandstand)", [77.5946, 12.9763]),
        ("Bangalore Palace", [77.5925, 12.9988]),
        ("Indiranagar Metro", [77.6386, 12.9784]),
    ]
    features = [feature(name, "landmark", {"type": "Point", "coordinates": coords}) for name, coords in points]
    features.append(
        feature(
            "Cubbon Park",
            "park",
            {
                "type": "Polygon",
                "coordinates": [[[77.5870, 12.9830], [77.6000, 12.9830], [77.6000, 12.9720], [77.5870, 12.9720], [77.5870, 12.9830]]],
            },
        )
    )
    features.append(
        feature(
            "Lalbagh",
            "park",
            {
                "type": "Polygon",
                "coordinates": [[[77.5790, 12.9560], [77.5900, 12.9560], [77.5900, 12.9450], [77.5790, 12.9450], [77.5790, 12.9560]]],
            },
        )
    )
    features.append(
        feature(
            "MG Road to Indiranagar",
            "road",
            {"type": "LineString", "coordinates": [[77.6074, 12.9756], [77.6250, 12.9770], [77.6400, 12.9784]]},
        )
    )
    return {"type": "FeatureCollection", "features": features}


def handler(ctx: context.NodeContext, workdir: str):
    log(f"GeoJSON demo source, version {NODE_VERSION}")
    collection = build_feature_collection()
    result_path = os.path.join(workdir, "result.geojson")
    with open(result_path, "w", encoding="utf-8") as handle:
        json.dump(collection, handle)

    for out in ctx.outputs:
        log(f"Uploading {len(collection['features'])} features to {out.path}")
        out.upload_file(result_path)

    return {"featureCount": len(collection["features"])}


if __name__ == "__main__":
    context.run(handler, prefix="[GEOJSON DEMO]")
