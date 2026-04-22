#!/usr/bin/env python3
"""
serve.py — Start a local HTTP server for the payroll dashboard.

Usage:
  python3 serve.py         # serves on http://localhost:8000
  python3 serve.py 9000    # serves on http://localhost:9000
"""
import http.server
import webbrowser
import sys
import os

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000

os.chdir(os.path.dirname(os.path.abspath(__file__)))

print(f"Starting server at http://localhost:{port}")
print("Press Ctrl+C to stop.")
webbrowser.open(f"http://localhost:{port}")

handler = http.server.SimpleHTTPRequestHandler
httpd = http.server.HTTPServer(("", port), handler)
try:
    httpd.serve_forever()
except KeyboardInterrupt:
    print("\nServer stopped.")
