#!/usr/bin/env python3
"""
dr_common.py

Shared, dependency-free helpers used by both generate_dr_mapping.py and
apply_config.py. Python standard library only — safe for air-gapped
Jenkins agents with no pip access.

Contains:
  - Simple flat-YAML reader for connect_config.yaml
  - DR mapping YAML reader/writer for primary-to-dr.yaml
  - urllib-based HTTP GET/PUT against the Kafka Connect REST API
"""

import base64
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request


# ============================================================
# connect_config.yaml (flat key: value, optional nested headers:)
# ============================================================

def load_simple_yaml(path):
    if not os.path.isfile(path):
        print(f"ERROR: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)

    result = {}
    current_nested_key = None

    def parse_scalar(raw):
        raw = raw.strip()
        if raw == "" or raw.lower() in ("null", "~"):
            return None
        if raw.lower() == "true":
            return True
        if raw.lower() == "false":
            return False
        if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
            return raw[1:-1]
        if len(raw) >= 2 and raw.startswith("'") and raw.endswith("'"):
            return raw[1:-1]
        try:
            return int(raw)
        except ValueError:
            pass
        return raw

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.split("#", 1)[0].rstrip("\n")
            if not line.strip():
                continue
            indent = len(line) - len(line.lstrip(" "))
            content = line.strip()

            if indent == 0:
                current_nested_key = None
                if ":" not in content:
                    continue
                key, _, value = content.partition(":")
                key = key.strip()
                value = value.strip()
                if value == "" or value == "{}":
                    result[key] = {}
                    current_nested_key = key
                else:
                    result[key] = parse_scalar(value)
            else:
                if current_nested_key is None or ":" not in content:
                    continue
                key, _, value = content.partition(":")
                result[current_nested_key][key.strip()] = parse_scalar(value)

    return result


def load_connection_config(config_path):
    cfg = load_simple_yaml(config_path)
    return {
        "connect_url": cfg.get("connect_url") or "http://localhost:8083",
        "user": cfg.get("user"),
        "password": cfg.get("password"),
        "insecure": bool(cfg.get("insecure", False)),
        "timeout": int(cfg.get("timeout", 15)) if cfg.get("timeout") is not None else 15,
        "headers": cfg.get("headers") or {},
    }


# ============================================================
# DR mapping YAML (primary-to-dr.yaml) — fixed schema reader/writer
# ============================================================

def _unquote(raw):
    raw = raw.strip()
    if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        return raw
    return raw


def parse_mapping_yaml(path):
    """
    Parses the fixed schema:

    version: 1
    direction: primary-to-dr
    connectors:
      jdbc:
        - name: "..."
          config_key: "..."
          primary_url: "..."
          dr_url: "..."
      mq:
        - name: "..."
          ...
    """
    if not os.path.isfile(path):
        print(f"ERROR: Mapping file not found: {path}", file=sys.stderr)
        sys.exit(1)

    result = {"version": None, "direction": None, "connectors": {"jdbc": [], "mq": []}}
    current_section = None
    current_entry = None

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line.strip() or line.strip().startswith("#"):
                continue

            indent = len(line) - len(line.lstrip(" "))
            content = line.strip()

            if indent == 0:
                if content.startswith("version:"):
                    result["version"] = _unquote(content.partition(":")[2])
                elif content.startswith("direction:"):
                    result["direction"] = _unquote(content.partition(":")[2])
                # "connectors:" line — nothing to do
                current_section = None
                current_entry = None
                continue

            if indent == 2:
                if content.startswith("jdbc:"):
                    current_section = "jdbc"
                elif content.startswith("mq:"):
                    current_section = "mq"
                current_entry = None
                continue

            if content == "[]":
                continue

            if content.startswith("- "):
                content = content[2:]
                current_entry = {}
                if current_section in ("jdbc", "mq"):
                    result["connectors"][current_section].append(current_entry)

            if current_entry is None or ":" not in content:
                continue

            key, _, value = content.partition(":")
            current_entry[key.strip()] = _unquote(value)

    return result


def yaml_quote(value):
    if value is None:
        return '""'
    value = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{value}"'


def write_dr_mapping_yaml(path, direction, jdbc_entries, mq_entries):
    lines = ["version: 1", f"direction: {direction}", "", "connectors:", "  jdbc:"]
    if not jdbc_entries:
        lines.append("    []")
    for e in jdbc_entries:
        lines.append(f"    - name: {yaml_quote(e['name'])}")
        lines.append(f"      config_key: {yaml_quote(e['config_key'])}")
        lines.append(f"      primary_url: {yaml_quote(e['primary_url'])}")
        lines.append(f"      dr_url: {yaml_quote(e['dr_url'])}")
    lines.append("")
    lines.append("  mq:")
    if not mq_entries:
        lines.append("    []")
    for e in mq_entries:
        lines.append(f"    - name: {yaml_quote(e['name'])}")
        lines.append(f"      config_key: {yaml_quote(e['config_key'])}")
        lines.append(f"      primary_url: {yaml_quote(e['primary_url'])}")
        lines.append(f"      dr_url: {yaml_quote(e['dr_url'])}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


# ============================================================
# HTTP / SSL (urllib only)
# ============================================================

def build_opener(insecure):
    if insecure:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return urllib.request.build_opener(urllib.request.HTTPSHandler(context=context))
    return urllib.request.build_opener()


def build_headers(user, password, extra_headers=None, json_body=False):
    headers = {"Accept": "application/json"}
    if json_body:
        headers["Content-Type"] = "application/json"
    if user and password:
        credentials = f"{user}:{password}"
        encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {encoded}"
    if extra_headers:
        for key, value in extra_headers.items():
            if value is not None:
                headers[str(key)] = str(value)
    return headers


def _request(opener, url, method, user, password, extra_headers, timeout, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url=url, method=method, data=data)
    headers = build_headers(user, password, extra_headers, json_body=(data is not None))
    for key, value in headers.items():
        request.add_header(key, value)

    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        try:
            response_body = error.read().decode("utf-8", errors="replace")
        except Exception:
            response_body = ""
        print("\nHTTP ERROR")
        print(f"URL      : {url}")
        print(f"Method   : {method}")
        print(f"HTTP Code: {error.code}")
        if response_body:
            print(f"Response : {response_body}")
        raise
    except urllib.error.URLError as error:
        print("\nCONNECTION ERROR")
        print(f"URL    : {url}")
        print(f"Reason : {error.reason}")
        raise


def http_get_json(opener, url, user, password, extra_headers, timeout):
    return _request(opener, url, "GET", user, password, extra_headers, timeout)


def http_put_json(opener, url, user, password, extra_headers, timeout, body):
    return _request(opener, url, "PUT", user, password, extra_headers, timeout, body=body)


def get_connector_config(opener, connect_url, name, user, password, headers, timeout):
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"{connect_url}/connectors/{encoded_name}/config"
    result = http_get_json(opener, url, user, password, headers, timeout)
    if not isinstance(result, dict):
        raise RuntimeError(f"Config response for {name} was not a JSON object.")
    return result


def put_connector_config(opener, connect_url, name, user, password, headers, timeout, new_config):
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"{connect_url}/connectors/{encoded_name}/config"
    return http_put_json(opener, url, user, password, headers, timeout, new_config)


def get_connector_status(opener, connect_url, name, user, password, headers, timeout):
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"{connect_url}/connectors/{encoded_name}/status"
    result = http_get_json(opener, url, user, password, headers, timeout)
    if not isinstance(result, dict):
        raise RuntimeError(f"Status response for {name} was not a JSON object.")
    return result
