import json
import os
import time
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Global in-memory state store for party member cooldowns
PARTY_STATES = {}

class PartyStatusHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Mute logging to keep stdout clean
        pass
        
    def do_POST(self):
        try:
            if self.path == "/update":
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode("utf-8"))
                
                player = data.get("player")
                skill = data.get("skill")
                is_ready = data.get("is_ready")
                cooldown_duration = data.get("cooldown_duration", 0)
                
                if player and skill is not None:
                    if player not in PARTY_STATES:
                        PARTY_STATES[player] = {}
                    PARTY_STATES[player][skill] = {
                        "is_ready": is_ready,
                        "timestamp": time.time(),
                        "cooldown_duration": cooldown_duration
                    }
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status":"success"}')
            elif self.path == "/clear":
                PARTY_STATES.clear()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status":"cleared"}')
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            try:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode('utf-8'))
            except Exception:
                pass

    def do_GET(self):
        if self.path == "/status":
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(PARTY_STATES).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        super().server_bind()


if __name__ == "__main__":
    # Render binds dynamic port via PORT environment variable, defaults to 10000
    port = int(os.environ.get("PORT", 10000))
    host = "0.0.0.0"
    print(f"Starting 펭구 줌인 Pro 중계 서버 on {host}:{port}...")
    
    try:
        httpd = ReusableThreadingHTTPServer((host, port), PartyStatusHandler)
        httpd.serve_forever()
    except Exception as e:
        print(f"Server error: {e}")
