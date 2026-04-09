from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        jobs_dir = Path("/app/jobs")
        jobs = sorted(p.name for p in jobs_dir.iterdir()) if jobs_dir.exists() else []
        body = json.dumps(
            {
                "service": "spark-api",
                "status": "ok",
                "jobs_dir": str(jobs_dir),
                "jobs": jobs,
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9003), Handler)
    server.serve_forever()
