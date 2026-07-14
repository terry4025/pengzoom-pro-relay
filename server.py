import json
import os
import time
import socket
import hashlib
import base64
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# Global lock and clients dictionary for WebSocket handling
STATE_LOCK = threading.Lock()
WEBSOCKET_CLIENTS = {}  # { room_id: set(handler) }

def read_ws_frame(rfile):
    first_byte = rfile.read(1)
    if not first_byte:
        return None, None
    header = first_byte[0]
    opcode = header & 0x0F
    
    second_byte = rfile.read(1)
    if not second_byte:
        return None, None
    mask_and_len = second_byte[0]
    is_masked = (mask_and_len & 0x80) != 0
    payload_len = mask_and_len & 0x7F
    
    if payload_len == 126:
        len_bytes = rfile.read(2)
        if len(len_bytes) < 2:
            return None, None
        payload_len = int.from_bytes(len_bytes, byteorder='big')
    elif payload_len == 127:
        len_bytes = rfile.read(8)
        if len(len_bytes) < 8:
            return None, None
        payload_len = int.from_bytes(len_bytes, byteorder='big')
        
    masking_key = b""
    if is_masked:
        masking_key = rfile.read(4)
        if len(masking_key) < 4:
            return None, None
            
    payload = rfile.read(payload_len)
    if len(payload) < payload_len:
        return None, None
        
    if is_masked:
        unmasked = bytearray(payload_len)
        for i in range(payload_len):
            unmasked[i] = payload[i] ^ masking_key[i % 4]
        payload = bytes(unmasked)
        
    return opcode, payload

def send_ws_message(wfile, text):
    try:
        payload = text.encode('utf-8')
        length = len(payload)
        
        frame = bytearray([0x81]) # FIN + Text
        if length < 126:
            frame.append(length)
        elif length < 65536:
            frame.append(126)
            frame.extend(length.to_bytes(2, byteorder='big'))
        else:
            frame.append(127)
            frame.extend(length.to_bytes(8, byteorder='big'))
            
        frame.extend(payload)
        wfile.write(frame)
        wfile.flush()
        return True
    except Exception:
        return False

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
        if parsed_url.path == "/ws":
            headers = self.headers
            if headers.get("Upgrade", "").lower() == "websocket":
                key = headers.get("Sec-WebSocket-Key")
                if not key:
                    self.send_response(400)
                    self.end_headers()
                    return
                
                accept_val = base64.b64encode(
                    hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode('utf-8')).digest()
                ).decode('utf-8')
                
                self.send_response(101, "Switching Protocols")
                self.send_header("Upgrade", "websocket")
                self.send_header("Connection", "Upgrade")
                self.send_header("Sec-WebSocket-Accept", accept_val)
                self.end_headers()
                
                self.handle_ws_connection()
                return
            else:
                self.send_response(400)
                self.end_headers()
        elif parsed_url.path == "/status":
            query_params = parse_qs(parsed_url.query)
            room_id = query_params.get("room_id", ["default"])[0]
            
            # Fetch states for the specified room only
            room_states = PARTY_STATES.get(room_id, {})
            
            response_data = {
                "server_time": time.time(),
                "states": room_states
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def handle_ws_connection(self):
        room_id = "default"
        player_name = "unknown"
        registered = False
        
        try:
            while True:
                opcode, payload = read_ws_frame(self.rfile)
                if opcode is None:
                    break
                if opcode == 8:  # Close
                    break
                elif opcode == 9:  # Ping
                    pong_frame = bytearray([0x8A, 0])
                    self.wfile.write(pong_frame)
                    self.wfile.flush()
                    continue
                elif opcode == 1:  # Text
                    msg_text = payload.decode('utf-8')
                    try:
                        msg = json.loads(msg_text)
                    except Exception:
                        continue
                    
                    action = msg.get("action")
                    if action == "join":
                        room_id = msg.get("room_id", "default")
                        player_name = msg.get("player", "unknown")
                        
                        with STATE_LOCK:
                            if room_id not in WEBSOCKET_CLIENTS:
                                WEBSOCKET_CLIENTS[room_id] = set()
                            WEBSOCKET_CLIENTS[room_id].add(self)
                            registered = True
                            
                            # Send initial room state
                            room_states = PARTY_STATES.get(room_id, {})
                            join_response = {
                                "type": "status",
                                "server_time": time.time(),
                                "states": room_states
                            }
                            send_ws_message(self.wfile, json.dumps(join_response))
                            
                    elif action == "update":
                        room_id = msg.get("room_id", "default")
                        player = msg.get("player")
                        skill = msg.get("skill")
                        is_ready = msg.get("is_ready")
                        cooldown_duration = msg.get("cooldown_duration", 0)
                        
                        if player and skill is not None:
                            with STATE_LOCK:
                                if room_id not in PARTY_STATES:
                                    PARTY_STATES[room_id] = {}
                                if player not in PARTY_STATES[room_id]:
                                    PARTY_STATES[room_id][player] = {}
                                    
                                PARTY_STATES[room_id][player][skill] = {
                                    "is_ready": is_ready,
                                    "timestamp": time.time(),
                                    "cooldown_duration": cooldown_duration
                                }
                                
                                # Broadcast update to all clients in the same room
                                broadcast_msg = {
                                    "type": "update",
                                    "server_time": time.time(),
                                    "player": player,
                                    "skill": skill,
                                    "state": PARTY_STATES[room_id][player][skill]
                                }
                                payload_str = json.dumps(broadcast_msg)
                                
                                dead_clients = set()
                                for client in WEBSOCKET_CLIENTS.get(room_id, []):
                                    if not send_ws_message(client.wfile, payload_str):
                                        dead_clients.add(client)
                                
                                if dead_clients:
                                    WEBSOCKET_CLIENTS[room_id].difference_update(dead_clients)
        except Exception:
            pass
        finally:
            if registered:
                with STATE_LOCK:
                    if room_id in WEBSOCKET_CLIENTS:
                        WEBSOCKET_CLIENTS[room_id].discard(self)


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
