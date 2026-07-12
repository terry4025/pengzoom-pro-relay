import json
import os
import time
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# Global in-memory state store for party member cooldowns, isolated by room_id
# Format: { room_id: { player_name: { skill_name: { is_ready: bool, timestamp: float, cooldown_duration: int } } } }
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
                
                room_id = data.get("room_id", "default")
                player = data.get("player")
                skill = data.get("skill")
                is_ready = data.get("is_ready")
                cooldown_duration = data.get("cooldown_duration", 0)
                
                if player and skill is not None:
                    if room_id not in PARTY_STATES:
                        PARTY_STATES[room_id] = {}
                    if player not in PARTY_STATES[room_id]:
                        PARTY_STATES[room_id][player] = {}
                        
                    PARTY_STATES[room_id][player][skill] = {
                        "is_ready": is_ready,
                        "timestamp": time.time(),
                        "cooldown_duration": cooldown_duration
                    }
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"status":"success"}')
                
            elif self.path == "/clear":
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)
                data = {}
                if content_length > 0:
                    try:
                        data = json.loads(post_data.decode("utf-8"))
                    except Exception:
                        pass
                
                room_id = data.get("room_id", "default")
                if room_id in PARTY_STATES:
                    PARTY_STATES[room_id].clear()
                    
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
        parsed_url = urlparse(self.path)
        if parsed_url.path == "/status":
            query_params = parse_qs(parsed_url.query)
            room_id = query_params.get("room_id", ["default"])[0]
            
            # Fetch states for the specified room only
            room_states = PARTY_STATES.get(room_id, {})
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(room_states).encode("utf-8"))
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
