"""A minimal in-memory stand-in for Confluent Schema Registry, for the native-stack benchmark.

The container that runs the benchmark cannot fetch Schema Registry, so this serves the endpoints the
PayStream clients use: register and look up a subject's schema, and fetch a schema by id. Clients
cache ids after the first call, so the registry is off the per-record path and does not affect the
numbers. No compatibility checks: the CI contract tests cover those.

    python benchmarks/stack/registry.py --port 8081
"""

from __future__ import annotations

import argparse
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CT = "application/vnd.schemaregistry.v1+json"


class Store:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.by_id: dict[int, str] = {}
        self.ids: dict[str, int] = {}  # canonical schema -> id
        self.subjects: dict[str, list[int]] = {}  # subject -> ids by version

    @staticmethod
    def canonical(schema: str) -> str:
        return json.dumps(json.loads(schema), sort_keys=True, separators=(",", ":"))

    def register(self, subject: str, schema: str) -> int:
        key = self.canonical(schema)
        with self.lock:
            sid = self.ids.get(key)
            if sid is None:
                sid = self.ids[key] = len(self.by_id) + 1
                self.by_id[sid] = schema
            versions = self.subjects.setdefault(subject, [])
            if sid not in versions:
                versions.append(sid)
            return sid

    def lookup(self, subject: str, schema: str) -> dict | None:
        sid = self.ids.get(self.canonical(schema))
        versions = self.subjects.get(subject, [])
        if sid is None or sid not in versions:
            return None
        return {"subject": subject, "id": sid, "version": versions.index(sid) + 1, "schema": self.by_id[sid]}


STORE = Store()


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", CT)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if m := re.fullmatch(r"/schemas/ids/(\d+)", path):
            schema = STORE.by_id.get(int(m[1]))
            return self._send(200, {"schema": schema}) if schema else self._send(404, {"error_code": 40403})
        if m := re.fullmatch(r"/subjects/([^/]+)/versions/latest", path):
            versions = STORE.subjects.get(m[1])
            if not versions:
                return self._send(404, {"error_code": 40401})
            sid = versions[-1]
            return self._send(200, {"subject": m[1], "id": sid, "version": len(versions), "schema": STORE.by_id[sid]})
        if path == "/subjects":
            return self._send(200, sorted(STORE.subjects))
        self._send(404, {"error_code": 404})

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        if m := re.fullmatch(r"/subjects/([^/]+)/versions", path):
            return self._send(200, {"id": STORE.register(m[1], self._body()["schema"])})
        if m := re.fullmatch(r"/subjects/([^/]+)", path):
            found = STORE.lookup(m[1], self._body()["schema"])
            return self._send(200, found) if found else self._send(404, {"error_code": 40403})
        self._send(404, {"error_code": 404})

    def do_PUT(self) -> None:
        self._send(200, self._body())

    def log_message(self, *args) -> None:
        pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8081)
    a = ap.parse_args()
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
