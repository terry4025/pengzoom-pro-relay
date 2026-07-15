import json
import os
import time
import socket
import hashlib
import base64
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlparse

# Global lock and clients dictionary for WebSocket handling
STATE_LOCK = threading.Lock()
WEBSOCKET_CLIENTS = {}  # { room_id: set(handler) }
# The currently authoritative WebSocket for each client id.  This prevents a
# late close/update from an older connection removing a newly connected player.
ROOM_CLIENT_CONNECTIONS = {}

LOSTARK_API_BASE_URL = "https://developer-lostark.game.onstove.com"
CHARACTER_PROFILE_CACHE_TTL = 60 * 60
CHARACTER_PROFILE_CACHE = {}
CHARACTER_PROFILE_CACHE_LOCK = threading.Lock()


class CharacterLookupError(Exception):
    def __init__(self, status_code, error_code, message):
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message


def validate_character_name(character_name):
    character_name = (character_name or "").strip()
    if not 1 <= len(character_name) <= 20:
        raise CharacterLookupError(400, "invalid_character_name", "캐릭터명을 확인해 주세요.")
    if not all(char.isalnum() for char in character_name):
        raise CharacterLookupError(400, "invalid_character_name", "캐릭터명에는 문자와 숫자만 사용할 수 있습니다.")
    return character_name


def lookup_character_profile(character_name):
    """Fetch only the public profile fields needed by the desktop client."""
    character_name = validate_character_name(character_name)
    cache_key = character_name.casefold()
    now = time.time()

    with CHARACTER_PROFILE_CACHE_LOCK:
        cached = CHARACTER_PROFILE_CACHE.get(cache_key)
        if cached and now - cached[0] < CHARACTER_PROFILE_CACHE_TTL:
            return cached[1]
        if cached:
            CHARACTER_PROFILE_CACHE.pop(cache_key, None)

    api_key = os.environ.get("LOSTARK_API_KEY", "").strip()
    if not api_key:
        raise CharacterLookupError(503, "api_key_not_configured", "캐릭터 자동 감지 기능을 준비 중입니다.")

    authorization = api_key if api_key.lower().startswith("bearer ") else f"bearer {api_key}"
    encoded_name = quote(character_name, safe="")
    url = f"{LOSTARK_API_BASE_URL}/armories/characters/{encoded_name}/profiles"
    request = urllib.request.Request(
        url,
        headers={
            "accept": "application/json",
            "authorization": authorization,
            "user-agent": "PengZoomPro-Relay/1.0",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise CharacterLookupError(404, "character_not_found", "캐릭터를 찾을 수 없습니다.") from error
        if error.code == 429:
            raise CharacterLookupError(429, "rate_limited", "잠시 후 다시 시도해 주세요.") from error
        if error.code in (401, 403):
            raise CharacterLookupError(503, "api_auth_failed", "캐릭터 자동 감지 기능을 준비 중입니다.") from error
        raise CharacterLookupError(502, "upstream_error", "로스트아크 API 응답을 처리하지 못했습니다.") from error
    except urllib.error.URLError as error:
        raise CharacterLookupError(502, "upstream_unavailable", "로스트아크 API에 연결할 수 없습니다.") from error
    except (TimeoutError, json.JSONDecodeError) as error:
        raise CharacterLookupError(502, "invalid_upstream_response", "로스트아크 API 응답을 처리하지 못했습니다.") from error

    if not isinstance(payload, dict):
        raise CharacterLookupError(502, "invalid_upstream_response", "로스트아크 API 응답을 처리하지 못했습니다.")

    character_class = payload.get("CharacterClassName")
    if not character_class:
        raise CharacterLookupError(404, "character_not_found", "캐릭터를 찾을 수 없습니다.")

    profile = {
        "character_name": payload.get("CharacterName") or character_name,
        "character_class": character_class,
        "server_name": payload.get("ServerName") or "",
        "character_level": payload.get("CharacterLevel"),
        "item_level": payload.get("ItemAvgLevel") or "",
    }
    with CHARACTER_PROFILE_CACHE_LOCK:
        CHARACTER_PROFILE_CACHE[cache_key] = (now, profile)
    return profile

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

# Map unique client_id to player_name to enforce one character per client.
ROOM_CLIENT_MAP = {}

def broadcast_ws_message_locked(room_id, message):
    """Broadcast while STATE_LOCK is held, pruning broken connections."""
    payload = json.dumps(message)
    dead_clients = set()
    for client in WEBSOCKET_CLIENTS.get(room_id, set()):
        if not send_ws_message(client.wfile, payload):
            dead_clients.add(client)
    if dead_clients:
        WEBSOCKET_CLIENTS.get(room_id, set()).difference_update(dead_clients)

def remove_player_locked(room_id, player_name):
    """Remove a player and immediately invalidate every connected UI cache."""
    room_states = PARTY_STATES.get(room_id)
    if room_states and room_states.pop(player_name, None) is not None:
        broadcast_ws_message_locked(room_id, {"type": "remove", "player": player_name})

def register_client_player(room_id, client_id, player_name, handler=None):
    """Make client_id own exactly one character in a room."""
    with STATE_LOCK:
        if not client_id:
            return
        if room_id not in ROOM_CLIENT_MAP:
            ROOM_CLIENT_MAP[room_id] = {}
        old_player = ROOM_CLIENT_MAP[room_id].get(client_id)
        if old_player and old_player != player_name:
            remove_player_locked(room_id, old_player)
        ROOM_CLIENT_MAP[room_id][client_id] = player_name
        if handler is not None:
            previous_handler = ROOM_CLIENT_CONNECTIONS.setdefault(room_id, {}).get(client_id)
            if previous_handler is not None and previous_handler is not handler:
                WEBSOCKET_CLIENTS.get(room_id, set()).discard(previous_handler)
            ROOM_CLIENT_CONNECTIONS[room_id][client_id] = handler

class PartyStatusHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Mute logging to keep stdout clean
        pass
        
    def do_POST(self):
        self.send_error(405, "This relay accepts WebSocket connections only")

    def send_json(self, status_code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed_url = urlparse(self.path)
        if parsed_url.path == "/character":
            character_name = parse_qs(parsed_url.query).get("name", [""])[0]
            try:
                profile = lookup_character_profile(character_name)
                self.send_json(200, profile)
            except CharacterLookupError as error:
                self.send_json(error.status_code, {
                    "error": error.error_code,
                    "message": error.message,
                })
            return
        elif parsed_url.path == "/ws":
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
        else:
            self.send_response(404)
            self.end_headers()

    def handle_ws_connection(self):
        room_id = "default"
        player_name = "unknown"
        connection_client_id = None
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
                        client_id = msg.get("client_id")
                        class_name = msg.get("class_name", "홀리나이트")
                        
                        register_client_player(room_id, client_id, player_name, self)
                        connection_client_id = client_id
                        
                        with STATE_LOCK:
                            if room_id not in WEBSOCKET_CLIENTS:
                                WEBSOCKET_CLIENTS[room_id] = set()
                            WEBSOCKET_CLIENTS[room_id].add(self)
                            registered = True
                            
                            if room_id not in PARTY_STATES:
                                PARTY_STATES[room_id] = {}
                            if player_name not in PARTY_STATES[room_id]:
                                PARTY_STATES[room_id][player_name] = {}
                            
                            PARTY_STATES[room_id][player_name]["_class"] = class_name
                            PARTY_STATES[room_id][player_name]["_client_id"] = client_id
                            
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
                        client_id = msg.get("client_id")
                        class_name = msg.get("class_name", "홀리나이트")
                        skill = msg.get("skill")
                        is_ready = msg.get("is_ready")
                        cooldown_duration = msg.get("cooldown_duration", 0)
                        
                        if player and skill is not None:
                            with STATE_LOCK:
                                # A replaced/older socket must never restore its old name.
                                if player != player_name or client_id != connection_client_id:
                                    continue
                                if client_id and ROOM_CLIENT_CONNECTIONS.get(room_id, {}).get(client_id) is not self:
                                    continue
                                if room_id not in PARTY_STATES:
                                    PARTY_STATES[room_id] = {}
                                if player not in PARTY_STATES[room_id]:
                                    PARTY_STATES[room_id][player] = {}
                                    
                                PARTY_STATES[room_id][player]["_class"] = class_name
                                PARTY_STATES[room_id][player]["_client_id"] = client_id
                                PARTY_STATES[room_id][player][skill] = {
                                    "is_ready": is_ready,
                                    "timestamp": time.time(),
                                    "cooldown_duration": cooldown_duration
                                }
                                
                                broadcast_ws_message_locked(room_id, {
                                    "type": "update",
                                    "server_time": time.time(),
                                    "player": player,
                                    "skill": skill,
                                    "state": PARTY_STATES[room_id][player][skill]
                                })
                    elif action == "class":
                        room_id = msg.get("room_id", "default")
                        player = msg.get("player")
                        client_id = msg.get("client_id")
                        class_name = msg.get("class_name")

                        if player and class_name:
                            with STATE_LOCK:
                                # Only the connection that joined as this player may change its class.
                                if player != player_name or client_id != connection_client_id:
                                    continue
                                if client_id and ROOM_CLIENT_CONNECTIONS.get(room_id, {}).get(client_id) is not self:
                                    continue
                                PARTY_STATES.setdefault(room_id, {}).setdefault(player, {})["_class"] = class_name
                                broadcast_ws_message_locked(room_id, {
                                    "type": "class",
                                    "player": player,
                                    "class_name": class_name,
                                })
        except Exception:
            pass
        finally:
            if registered:
                with STATE_LOCK:
                    if room_id in WEBSOCKET_CLIENTS:
                        WEBSOCKET_CLIENTS[room_id].discard(self)
                    current_handler = ROOM_CLIENT_CONNECTIONS.get(room_id, {}).get(connection_client_id)
                    # Ignore a late close from a superseded connection.
                    if connection_client_id and current_handler is not self:
                        return
                    if connection_client_id:
                        ROOM_CLIENT_CONNECTIONS.get(room_id, {}).pop(connection_client_id, None)
                    if ROOM_CLIENT_MAP.get(room_id, {}).get(connection_client_id) == player_name:
                        ROOM_CLIENT_MAP[room_id].pop(connection_client_id, None)
                    remove_player_locked(room_id, player_name)


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
