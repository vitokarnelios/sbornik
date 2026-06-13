#!/usr/bin/env python3
import os
import requests
import base64
import random
import json
import subprocess
import time
import queue
import threading
import re
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FINAL_DIR = os.path.join(BASE_PATH, "subs")
os.makedirs(FINAL_DIR, exist_ok=True)

SOURCES_FILE = os.path.join(BASE_PATH, "sources.txt")

if not os.path.exists(SOURCES_FILE):
    print("Файл sources.txt не найден")
    exit(1)

with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    SOURCES = [
        line.strip()
        for line in f
        if line.strip() and not line.strip().startswith("#")
    ]

MAX_NODES = 100       
MAX_THREADS = 40      
stop_event = threading.Event()

port_queue = queue.Queue()
for i in range(MAX_THREADS):
    port_queue.put(11000 + i)

TEST_URLS = [
    "https://vk.com",
    "https://yandex.ru",
    "https://www.microsoft.com",
    "https://google.com",
]

def decode_base64_content(text):
    try:
        text = text.strip()
        if "://" in text:
            return [text]
        padding = len(text) % 4
        if padding:
            text += "=" * (4 - padding)
        decoded = base64.b64decode(text).decode("utf-8", errors="ignore")
        return decoded.splitlines()
    except:
        return []

def fetch_source(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        r = requests.get(url, timeout=15, headers=headers)
        if r.status_code == 200:
            final_lines = []
            for line in r.text.splitlines():
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('{'):
                    try:
                        config = json.loads(line)
                        if 'outbounds' in config:
                            for outbound in config['outbounds']:
                                if outbound.get('protocol') == 'vless':
                                    uri = convert_json_to_vless_uri(outbound, config.get('remarks', ''))
                                    if uri:
                                        final_lines.append(uri)
                    except:
                        pass
                
                if "://" not in line and len(line) > 50:
                    final_lines.extend(decode_base64_content(line))
                else:
                    final_lines.append(line)
            return final_lines
    except Exception as e:
        print(f"Ошибка загрузки {url}: {e}")
    return []

def convert_json_to_vless_uri(outbound, remark=""):
    try:
        settings = outbound.get('settings', {})
        vnext = settings.get('vnext', [])
        if not vnext:
            return None
        
        server = vnext[0]
        address = server.get('address')
        port = server.get('port', 443)
        users = server.get('users', [])
        if not users:
            return None
        
        user = users[0]
        uuid = user.get('id')
        
        stream = outbound.get('streamSettings', {})
        network = stream.get('network', 'tcp')
        security = stream.get('security', '')
        
        params = {
            'encryption': 'none',
            'type': network,
        }
        
        if security == 'reality' or 'realitySettings' in stream:
            reality = stream.get('realitySettings', {})
            if reality:
                params['security'] = 'reality'
                params['pbk'] = reality.get('publicKey', '')
                params['sni'] = reality.get('serverName', '')
                params['sid'] = reality.get('shortId', '')
                params['fp'] = reality.get('fingerprint', 'chrome')
        
        elif security == 'tls':
            tls = stream.get('tlsSettings', {})
            params['security'] = 'tls'
            params['sni'] = tls.get('serverName', '')
            params['fp'] = tls.get('fingerprint', 'chrome')
            alpn = tls.get('alpn', [])
            if alpn:
                params['alpn'] = ','.join(alpn)
        
        if network == 'grpc':
            grpc = stream.get('grpcSettings', {})
            params['serviceName'] = grpc.get('serviceName', '')
        
        elif network == 'xhttp':
            xhttp = stream.get('xhttpSettings', {})
            params['path'] = xhttp.get('path', '/')
            host = xhttp.get('host', '')
            if host:
                params['host'] = host
            params['mode'] = xhttp.get('mode', 'auto')
        
        elif network == 'ws':
            ws = stream.get('wsSettings', {})
            params['path'] = ws.get('path', '/')
            host = ws.get('headers', {}).get('Host', '')
            if host:
                params['host'] = host
        
        query = '&'.join([f"{k}={v}" for k, v in params.items() if v])
        fragment = remark or outbound.get('tag', 'vless-node')
        
        uri = f"vless://{uuid}@{address}:{port}?{query}#{fragment}"
        return uri
    except Exception as e:
        return None

def is_valid_vless(line):
    return line.lower().startswith("vless://")

def parse_vless_to_json(vless_uri, listen_port):
    try:
        parsed = urlparse(vless_uri)
        netloc = parsed.netloc
        if '@' not in netloc:
            return None
        
        uuid, server_part = netloc.split('@', 1)
        if ':' in server_part:
            server_address, server_port_str = server_part.split(':', 1)
            server_port = int(server_port_str)
        else:
            server_address = server_part
            server_port = 443
        
        query_params = parse_qs(parsed.query)
        params = {k.lower(): v[0] for k, v in query_params.items() if v}
        
        node_name = unquote(parsed.fragment) if parsed.fragment else "vless-node"
        
        outbound = {
            "type": "vless",
            "tag": node_name,
            "server": server_address,
            "server_port": server_port,
            "uuid": uuid,
        }
        
        flow = params.get("flow", "")
        if flow:
            outbound["flow"] = flow
        
        security = params.get("security", "")
        transport_type = params.get("type", "tcp")
        
        if "pbk" in params or params.get("security") == "reality" or security == "reality":
            outbound["tls"] = {
                "enabled": True,
                "server_name": params.get("sni", server_address),
                "utls": {
                    "enabled": True,
                    "fingerprint": params.get("fp", "chrome")
                },
                "reality": {
                    "enabled": True,
                    "public_key": params.get("pbk", ""),
                    "short_id": params.get("sid", "")
                }
            }
        
        elif security == "tls" or params.get("security") == "tls":
            outbound["tls"] = {
                "enabled": True,
                "server_name": params.get("sni", server_address),
                "utls": {
                    "enabled": True,
                    "fingerprint": params.get("fp", "chrome")
                }
            }
            if "alpn" in params:
                outbound["tls"]["alpn"] = params["alpn"].split(',')
        
        if transport_type == "grpc":
            outbound["transport"] = {
                "type": "grpc",
                "service_name": params.get("serviceName", "")
            }
        
        elif transport_type == "xhttp":
            outbound["transport"] = {
                "type": "xhttp",
                "path": params.get("path", "/"),
                "host": [params["host"]] if params.get("host") else [],
                "mode": params.get("mode", "auto")
            }
        
        elif transport_type == "ws":
            outbound["transport"] = {
                "type": "ws",
                "path": params.get("path", "/"),
                "headers": {
                    "Host": params.get("host", server_address)
                }
            }
        
        config = {
            "log": {"level": "error"},
            "inbounds": [
                {
                    "type": "socks",
                    "tag": "socks-in",
                    "listen": "127.0.0.1",
                    "listen_port": listen_port
                }
            ],
            "outbounds": [outbound]
        }
        return config
    except Exception as e:
        return None

def check_node_worker(vless_uri):
    if stop_event.is_set():
        return None

    local_port = port_queue.get()
    temp_config_path = os.path.join(BASE_PATH, f"temp_{local_port}.json")
    temp_log_path = os.path.join(BASE_PATH, f"singbox_{local_port}.log")
    
    config = parse_vless_to_json(vless_uri, local_port)
    if not config:
        port_queue.put(local_port)
        return None

    with open(temp_config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)

    proc = None
    try:
        log_file = open(temp_log_path, "w", encoding="utf-8")
        proc = subprocess.Popen(
            ["sing-box", "run", "-c", temp_config_path],
            stdout=log_file,
            stderr=log_file
        )
        
        time.sleep(2.5)
        
        if stop_event.is_set():
            return None

        if proc.poll() is not None:
            return None

        proxies = {
            "http": f"socks5h://127.0.0.1:{local_port}",
            "https": f"socks5h://127.0.0.1:{local_port}"
        }

        for url in TEST_URLS:
            if stop_event.is_set():
                break
            try:
                response = requests.get(
                    url,
                    proxies=proxies,
                    timeout=4,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                )
                if response.status_code in [200, 204, 301, 302]:
                    if stop_event.is_set():
                        break
                    print(f"[LIVE] {url[:20]} (порт {local_port})")
                    return vless_uri
            except:
                continue
    except Exception as e:
        print(f"Ошибка проверки: {e}")
    finally:
        try:
            log_file.close()
        except:
            pass
        
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                proc.kill()
        
        for f in [temp_config_path, temp_log_path]:
            if os.path.exists(f):
                os.remove(f)
        
        port_queue.put(local_port)
    
    return None

def main():
    try:
        sb_v = subprocess.run(["sing-box", "version"], capture_output=True, text=True)
        print(f"=== SING-BOX STATUS ===\n{sb_v.stdout.strip()}\n======================")
    except Exception as e:
        print(f"Ошибка: sing-box не найден! {e}")
        return

    print("--- STEP 1: FETCHING SOURCES ---")
    all_nodes = []
    
    for url in SOURCES:
        nodes = fetch_source(url)
        print(f"Loaded {len(nodes)} lines from {url[:50]}...")
        all_nodes.extend(nodes)

    print(f"\n--- STEP 2: VALIDATION ---")
    unique_nodes = []
    seen = set()
    
    for line in all_nodes:
        line = line.strip()
        if not is_valid_vless(line):
            continue
        if line in seen:
            continue
        seen.add(line)
        unique_nodes.append(line)
    
    print(f"Unique VLESS configs: {len(unique_nodes)}")
    
    archive_path = os.path.join(BASE_PATH, "archive.txt")
    alive_archive_path = os.path.join(BASE_PATH, "alive_archive.txt")
    
    archive_list = []
    alive_archive_list = []
    
    if os.path.exists(archive_path):
        with open(archive_path, "r", encoding="utf-8") as f:
            archive_list = [x.strip() for x in f if x.strip()]
    
    if os.path.exists(alive_archive_path):
        with open(alive_archive_path, "r", encoding="utf-8") as f:
            alive_archive_list = [x.strip() for x in f if x.strip()]
    
    archive_seen = set(archive_list)
    for node in unique_nodes:
        if node not in archive_seen:
            archive_list.append(node)
            archive_seen.add(node)
    
    if len(archive_list) > 10000:
        archive_list = archive_list[-10000:]
    
    with open(archive_path, "w", encoding="utf-8") as f:
        f.write("\n".join(archive_list))
    
    print(f"Archive updated: {len(archive_list)} nodes (limit 10000)")
    
    priority_order = []
    for node in reversed(alive_archive_list):
        if node in seen and node not in priority_order:
            priority_order.append(node)
    
    for node in unique_nodes:
        if node not in priority_order:
            priority_order.append(node)
    
    print(f"\n--- STEP 3: LIVE CHECK ({len(priority_order)} nodes, threads: {MAX_THREADS}) ---")
    alive_nodes = []
    stop_event.clear()
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_node_worker, node) for node in priority_order]
        
        for future in as_completed(futures):
            try:
                res = future.result()
                if res:
                    alive_nodes.append(res)
                    # ИСПРАВЛЕНИЕ: убрал остановку, проверяем все ноды
            except Exception as e:
                print(f"Future error: {e}")
    
    print(f"\n--- RESULT: Found {len(alive_nodes)} live nodes ---")
    
    alive_nodes = list(dict.fromkeys(alive_nodes))
    print(f"After dedup: {len(alive_nodes)}")
    
    if len(alive_nodes) < MAX_NODES:
        added = 0
        for node in reversed(alive_archive_list):
            if node not in alive_nodes:
                alive_nodes.append(node)
                added += 1
                if len(alive_nodes) >= MAX_NODES:
                    break
        print(f"Filled from archive (+{added}), total: {len(alive_nodes)}")
    
    for node in alive_nodes:
        if node in alive_archive_list:
            alive_archive_list.remove(node)
        alive_archive_list.append(node)
    
    if len(alive_archive_list) > 5000:
        alive_archive_list = alive_archive_list[-5000:]
    
    with open(alive_archive_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_archive_list))
    
    print(f"alive_archive.txt updated: {len(alive_archive_list)} nodes (limit 5000)")
    
    if len(alive_nodes) == 0:
        print("WARNING: 0 live nodes, skipping update")
        return
    
    out_path = os.path.join(FINAL_DIR, "vless_001.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(alive_nodes[:MAX_NODES]))
    
    print(f"Subscription updated: {out_path}")
    
    types_count = {"reality": 0, "tls": 0, "grpc": 0, "xhttp": 0, "ws": 0, "other": 0}
    for node in alive_nodes:
        if "security=reality" in node or "pbk=" in node:
            types_count["reality"] += 1
        elif "type=grpc" in node:
            types_count["grpc"] += 1
        elif "type=xhttp" in node:
            types_count["xhttp"] += 1
        elif "type=ws" in node:
            types_count["ws"] += 1
        elif "security=tls" in node:
            types_count["tls"] += 1
        else:
            types_count["other"] += 1
    
    print(f"\n--- STATS BY PROTOCOL ---")
    for proto, count in types_count.items():
        if count > 0:
            print(f"{proto}: {count}")

if __name__ == "__main__":
    main()
