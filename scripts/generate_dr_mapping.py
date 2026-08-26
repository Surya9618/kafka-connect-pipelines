#!/usr/bin/env python3
"""
generate_dr_mapping.py  (zero external dependencies — stdlib only)

Extracts current connector configs from a Kafka Connect REST API and
populates a DR mapping YAML file matching the agreed schema:

version: 1
direction: primary-to-dr
connectors:
  jdbc:
    - name: <connector>
      config_key: connection.url
      primary_url: <current value, auto-filled>
      dr_url: ""            # left blank for manual fill
  mq:
    - name: <connector>
      config_key: mq.hostname
      primary_url: <current value, auto-filled>
      dr_url: ""            # left blank for manual fill

WHY NO requests / pyyaml:
  Air-gapped or restricted Jenkins agents often can't reach PyPI, and
  installing requests drags in transitive deps (charset_normalizer, idna,
  urllib3, certifi) that also need to be resolved offline. This version
  uses only Python's standard library (urllib.request for HTTP, json for
  the REST responses, and a small hand-rolled reader/writer for our fixed
  YAML shape) so it runs on any Python 3.6+ install with nothing to pip
  install at all.

Connection settings (connect_url, auth, insecure, timeout, headers) come
from an external config file (default: connect_config.yaml, next to this
script), so switching environments never requires editing this script.

Usage:
  python3 generate_dr_mapping.py --output primary-to-dr.yaml
  python3 generate_dr_mapping.py --config prod_connect_config.yaml --output primary-to-dr.yaml
  python3 generate_dr_mapping.py --connect-url http://otherhost:8083 --output test.yaml
  python3 generate_dr_mapping.py --connectors jdbc-source-orders,mq-sink-payments --output primary-to-dr.yaml

Type detection:
  - JDBC: "connector.class" contains "jdbc" (case-insensitive) AND config
    contains "connection.url".
  - MQ: config contains "mq.hostname".
  - Anything else -> "unclassified" and skipped; handle those manually.
"""

import argparse
import base64
import json
import os
import ssl
import sys
import urllib.error
import urllib.request


JDBC_CONFIG_KEY = "connection.url"
MQ_CONFIG_KEY = "mq.hostname"

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "connect_config.yaml")


# --------------------------------------------------------------------------
# Minimal YAML reader — only supports the flat key: value shape used by
# connect_config.yaml (plus a simple nested "headers:" block). Not a
# general-purpose YAML parser; deliberately kept small and dependency-free.
# --------------------------------------------------------------------------
def load_simple_yaml(path):
    if not os.path.isfile(path):
        print(f"ERROR: config file not found: {path}", file=sys.stderr)
        print("Create it (see connect_config.yaml example) or pass --config <path>.", file=sys.stderr)
        sys.exit(1)

    result = {}
    current_nested_key = None

    def parse_scalar(raw):
        raw = raw.strip()
        if raw == "" or raw.lower() == "null" or raw == "~":
            return None
        if raw.lower() == "true":
            return True
        if raw.lower() == "false":
            return False
        if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
            return raw[1:-1]
        if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
            return raw[1:-1]
        # int
        try:
            return int(raw)
        except ValueError:
            pass
        return raw

    with open(path, "r") as f:
        for line in f:
            stripped = line.split("#", 1)[0].rstrip("\n")
            if not stripped.strip():
                continue

            indent = len(stripped) - len(stripped.lstrip(" "))
            content = stripped.strip()

            if indent == 0:
                current_nested_key = None
                if ":" not in content:
                    continue
                key, _, value = content.partition(":")
                key = key.strip()
                value = value.strip()
                if value == "" or value == "{}":
                    # Could be start of nested block (e.g. "headers:")
                    result[key] = {}
                    current_nested_key = key
                else:
                    result[key] = parse_scalar(value)
            else:
                # nested line, e.g. under "headers:"
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


# --------------------------------------------------------------------------
# Minimal YAML writer for our fixed output shape (version/direction/connectors)
# --------------------------------------------------------------------------
def yaml_quote(value):
    if value is None:
        return '""'
    s = str(value)
    # Always quote to keep things unambiguous (URLs contain ':' which is
    # special in YAML), and escape any embedded double quotes.
    s = s.replace('"', '\\"')
    return f'"{s}"'


def write_dr_mapping_yaml(path, direction, jdbc_entries, mq_entries):
    lines = []
    lines.append("version: 1")
    lines.append(f"direction: {direction}")
    lines.append("connectors:")

    lines.append("  jdbc:")
    if not jdbc_entries:
        lines.append("    []")
    for e in jdbc_entries:
        lines.append(f"    - name: {yaml_quote(e['name'])}")
        lines.append(f"      config_key: {yaml_quote(e['config_key'])}")
        lines.append(f"      primary_url: {yaml_quote(e['primary_url'])}")
        lines.append(f"      dr_url: {yaml_quote(e['dr_url'])}")

    lines.append("  mq:")
    if not mq_entries:
        lines.append("    []")
    for e in mq_entries:
        lines.append(f"    - name: {yaml_quote(e['name'])}")
        lines.append(f"      config_key: {yaml_quote(e['config_key'])}")
        lines.append(f"      primary_url: {yaml_quote(e['primary_url'])}")
        lines.append(f"      dr_url: {yaml_quote(e['dr_url'])}")

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


# --------------------------------------------------------------------------
# HTTP helpers (stdlib urllib only)
# --------------------------------------------------------------------------
def build_opener(insecure):
    if insecure:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))
    return urllib.request.build_opener()


def http_get_json(opener, url, user, password, extra_headers, timeout):
    req = urllib.request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    if user and password:
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    for k, v in (extra_headers or {}).items():
        req.add_header(k, v)

    with opener.open(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def get_connector_names(opener, connect_url, user, password, headers, timeout):
    return http_get_json(opener, f"{connect_url}/connectors", user, password, headers, timeout)


def get_connector_config(opener, connect_url, name, user, password, headers, timeout):
    return http_get_json(opener, f"{connect_url}/connectors/{name}/config", user, password, headers, timeout)


def classify(config):
    connector_class = config.get("connector.class", "") or ""
    if "jdbc" in connector_class.lower() and JDBC_CONFIG_KEY in config:
        return "jdbc", JDBC_CONFIG_KEY
    if MQ_CONFIG_KEY in config:
        return "mq", MQ_CONFIG_KEY
    return None, None


def build_entry(name, config_key, config):
    return {
        "name": name,
        "config_key": config_key,
        "primary_url": config.get(config_key, ""),
        "dr_url": "",  # fill in manually
    }


def main():
    parser = argparse.ArgumentParser(description="Generate DR mapping YAML from Kafka Connect configs (stdlib only)")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH,
                         help=f"Path to connection config YAML (default: {DEFAULT_CONFIG_PATH})")
    parser.add_argument("--connect-url", help="Override connect_url from config file")
    parser.add_argument("--user", help="Override user from config file")
    parser.add_argument("--password", help="Override password from config file")
    parser.add_argument("--insecure", action="store_true", default=None, help="Override insecure=true for this run")
    parser.add_argument("--timeout", type=int, help="Override timeout (seconds) from config file")
    parser.add_argument("--connectors", help="Comma-separated connector names. Default: all connectors.")
    parser.add_argument("--output", default="primary-to-dr.yaml", help="Output YAML file path")
    parser.add_argument("--direction", default="primary-to-dr", help="Value for the 'direction' field in the YAML")
    args = parser.parse_args()

    conn_cfg = load_connection_config(args.config)

    if args.connect_url:
        conn_cfg["connect_url"] = args.connect_url
    if args.user:
        conn_cfg["user"] = args.user
    if args.password:
        conn_cfg["password"] = args.password
    if args.insecure is not None:
        conn_cfg["insecure"] = args.insecure
    if args.timeout is not None:
        conn_cfg["timeout"] = args.timeout

    connect_url = conn_cfg["connect_url"].rstrip("/")
    timeout = conn_cfg["timeout"]
    opener = build_opener(conn_cfg["insecure"])

    print(f"Using config file: {args.config}")
    print(f"Kafka Connect URL: {connect_url}  (insecure={conn_cfg['insecure']})")

    if args.connectors:
        names = [n.strip() for n in args.connectors.split(",") if n.strip()]
    else:
        print(f"Fetching connector list from {connect_url}/connectors ...")
        try:
            names = get_connector_names(opener, connect_url, conn_cfg["user"], conn_cfg["password"],
                                         conn_cfg["headers"], timeout)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            print(f"ERROR: could not reach Kafka Connect at {connect_url}: {e}", file=sys.stderr)
            sys.exit(1)

    print(f"Found {len(names)} connector(s) to process.")

    jdbc_entries = []
    mq_entries = []
    unclassified = []

    for name in names:
        try:
            config = get_connector_config(opener, connect_url, name, conn_cfg["user"], conn_cfg["password"],
                                           conn_cfg["headers"], timeout)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            print(f"  [WARN] Could not fetch config for '{name}': {e}", file=sys.stderr)
            unclassified.append(name)
            continue

        conn_type, config_key = classify(config)

        if conn_type is None:
            print(f"  [WARN] '{name}' did not match jdbc or mq pattern — skipped, handle manually.")
            unclassified.append(name)
            continue

        entry = build_entry(name, config_key, config)
        if conn_type == "jdbc":
            jdbc_entries.append(entry)
        else:
            mq_entries.append(entry)
        print(f"  [OK] {name}  ->  type={conn_type}, config_key={config_key}, primary_url='{entry['primary_url']}'")

    write_dr_mapping_yaml(args.output, args.direction, jdbc_entries, mq_entries)

    print(f"\nWritten: {args.output}")
    print(f"  jdbc connectors: {len(jdbc_entries)}")
    print(f"  mq connectors:   {len(mq_entries)}")
    if unclassified:
        print(f"  Unclassified / skipped: {len(unclassified)} -> {', '.join(unclassified)}")
    print("\nNote: 'dr_url' fields are left blank — fill them in manually before running the DR switch pipeline.")


if __name__ == "__main__":
    main()
