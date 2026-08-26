#!/usr/bin/env python3
"""
apply_config.py  (stdlib-only, no pip install required)

Reads primary-to-dr.yaml and, depending on --direction, treats either
primary_url (direction=primary-to-dr) or dr_url (direction=dr-to-primary)
as the SOURCE value and the other as the TARGET value.

Stages (called separately from Jenkinsfile so failures are isolated
and logs are readable per-stage):

  --validate-only   Fetch live config, compare against expected source
                     value. Warns on drift, does not change anything.
  --backup-only      Fetch and save full current config per connector
                     to backups/<timestamp>/<connector>.json
  --diff-only        Print current -> target value per connector.
  --apply            Actually PUT the new config (target value) for
                     connectors not already at target. Idempotent —
                     skips connectors already switched.
  --verify           Poll connector status until RUNNING (or timeout).
  --commit-audit      git add + commit the backups/ dir and a summary
                     file. Does NOT push unless --push is also given.

Usage:
  python3 apply_config.py --direction primary-to-dr --validate-only
  python3 apply_config.py --direction primary-to-dr --backup-only
  python3 apply_config.py --direction primary-to-dr --diff-only
  python3 apply_config.py --direction primary-to-dr --apply
  python3 apply_config.py --direction primary-to-dr --verify
  python3 apply_config.py --direction primary-to-dr --commit-audit
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dr_common as common


DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "connect_config.yaml")
DEFAULT_MAPPING_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "primary-to-dr.yaml")


def flatten_entries(mapping):
    entries = []
    for conn_type in ("jdbc", "mq"):
        for e in mapping["connectors"].get(conn_type, []):
            e = dict(e)
            e["type"] = conn_type
            entries.append(e)
    return entries


def resolve_source_target(entry, direction):
    """
    Returns (source_value, target_value) for this entry given the direction.
    """
    if direction == "primary-to-dr":
        return entry["primary_url"], entry["dr_url"]
    elif direction == "dr-to-primary":
        return entry["dr_url"], entry["primary_url"]
    else:
        raise ValueError(f"Unknown direction: {direction}")


def filter_connectors(entries, connector_list):
    if not connector_list or connector_list.strip().upper() == "ALL":
        return entries
    wanted = {n.strip() for n in connector_list.split(",") if n.strip()}
    filtered = [e for e in entries if e["name"] in wanted]
    missing = wanted - {e["name"] for e in filtered}
    if missing:
        print(f"WARNING: connectors not found in mapping file: {', '.join(sorted(missing))}", file=sys.stderr)
    return filtered


def cmd_validate(entries, direction, opener, conn_cfg, connect_url):
    print(f"Validating {len(entries)} connector(s) for direction={direction}\n")
    problems = 0
    for e in entries:
        source, target = resolve_source_target(e, direction)
        try:
            live = common.get_connector_config(
                opener, connect_url, e["name"], conn_cfg["user"], conn_cfg["password"],
                conn_cfg["headers"], conn_cfg["timeout"]
            )
        except Exception as err:
            print(f"[ERROR] {e['name']}: could not fetch live config: {err}")
            problems += 1
            continue

        current_value = live.get(e["config_key"], "")

        if current_value == target:
            print(f"[SKIP]  {e['name']}: already at target value ({target}) — nothing to do")
        elif current_value == source:
            print(f"[OK]    {e['name']}: matches expected source value ({source})")
        else:
            print(f"[DRIFT] {e['name']}: current value '{current_value}' matches neither "
                  f"source '{source}' nor target '{target}' — needs manual review")
            problems += 1

    print()
    if problems:
        print(f"Validation found {problems} issue(s).")
        sys.exit(1)
    print("Validation passed.")


def cmd_backup(entries, opener, conn_cfg, connect_url, direction):
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = os.path.join("backups", f"{direction}-{timestamp}")
    os.makedirs(backup_dir, exist_ok=True)

    print(f"Backing up {len(entries)} connector(s) to {backup_dir}\n")
    failed = []
    for e in entries:
        try:
            live = common.get_connector_config(
                opener, connect_url, e["name"], conn_cfg["user"], conn_cfg["password"],
                conn_cfg["headers"], conn_cfg["timeout"]
            )
        except Exception as err:
            print(f"[ERROR] {e['name']}: backup failed: {err}")
            failed.append(e["name"])
            continue

        out_path = os.path.join(backup_dir, f"{e['name']}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(live, f, indent=2, sort_keys=True)
        print(f"[OK]    {e['name']} -> {out_path}")

    print(f"\nBackup directory: {backup_dir}")
    if failed:
        print(f"Failed to back up: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)


def cmd_diff(entries, direction):
    print(f"Planned changes for direction={direction}\n")
    for e in entries:
        source, target = resolve_source_target(e, direction)
        print(f"  {e['name']} [{e['type']}]")
        print(f"    {e['config_key']}: {source}  ->  {target}")
    print(f"\n{len(entries)} connector(s) in scope.")


def cmd_apply(entries, direction, opener, conn_cfg, connect_url):
    print(f"Applying direction={direction} to {len(entries)} connector(s)\n")
    changed, skipped, failed = [], [], []

    for e in entries:
        source, target = resolve_source_target(e, direction)
        try:
            live = common.get_connector_config(
                opener, connect_url, e["name"], conn_cfg["user"], conn_cfg["password"],
                conn_cfg["headers"], conn_cfg["timeout"]
            )
        except Exception as err:
            print(f"[ERROR] {e['name']}: could not fetch live config, skipping apply: {err}")
            failed.append(e["name"])
            continue

        current_value = live.get(e["config_key"], "")

        if current_value == target:
            print(f"[SKIP]  {e['name']}: already at target — idempotent no-op")
            skipped.append(e["name"])
            continue

        new_config = dict(live)
        new_config[e["config_key"]] = target

        try:
            common.put_connector_config(
                opener, connect_url, e["name"], conn_cfg["user"], conn_cfg["password"],
                conn_cfg["headers"], conn_cfg["timeout"], new_config
            )
        except Exception as err:
            print(f"[ERROR] {e['name']}: PUT failed: {err}")
            failed.append(e["name"])
            continue

        print(f"[OK]    {e['name']}: {e['config_key']} set to {target}")
        changed.append(e["name"])

    print(f"\nChanged: {len(changed)}  Skipped (already correct): {len(skipped)}  Failed: {len(failed)}")
    if failed:
        print(f"Failed connectors: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)


def cmd_verify(entries, opener, conn_cfg, connect_url, timeout_seconds=120, poll_interval=5):
    print(f"Verifying {len(entries)} connector(s) reach RUNNING state (timeout {timeout_seconds}s)\n")
    pending = {e["name"] for e in entries}
    deadline = time.time() + timeout_seconds
    failed = []

    while pending and time.time() < deadline:
        for name in list(pending):
            try:
                status = common.get_connector_status(
                    opener, connect_url, name, conn_cfg["user"], conn_cfg["password"],
                    conn_cfg["headers"], conn_cfg["timeout"]
                )
            except Exception as err:
                print(f"[ERROR] {name}: status check failed: {err}")
                continue

            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            task_states = [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]

            if connector_state == "RUNNING" and all(s == "RUNNING" for s in task_states):
                print(f"[OK]    {name}: RUNNING (tasks: {task_states or 'none'})")
                pending.discard(name)
            elif connector_state == "FAILED" or "FAILED" in task_states:
                print(f"[FAILED] {name}: connector={connector_state} tasks={task_states}")
                failed.append(name)
                pending.discard(name)

        if pending:
            time.sleep(poll_interval)

    if pending:
        print(f"\nTimed out waiting for: {', '.join(pending)}")
        failed.extend(pending)

    print()
    if failed:
        print(f"Verification failed for: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)
    print("All connectors verified RUNNING.")


def cmd_commit_audit(direction, push=False):
    summary_path = "backups/LAST_RUN.txt"
    os.makedirs("backups", exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"direction: {direction}\n")
        f.write(f"timestamp_utc: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    commands = [
        ["git", "add", "backups/"],
        ["git", "commit", "-m", f"DR audit: {direction} at {datetime.datetime.now(datetime.timezone.utc).isoformat()}"],
    ]
    if push:
        commands.append(["git", "push"])

    for cmd in commands:
        print(f"$ {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout.strip())
        if result.returncode != 0:
            # "nothing to commit" is not a hard failure
            if "nothing to commit" in (result.stdout + result.stderr).lower():
                print("(nothing new to commit)")
                continue
            print(result.stderr.strip(), file=sys.stderr)
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Apply Kafka Connect DR hostname switch (stdlib-only)")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to connect_config.yaml")
    parser.add_argument("--mapping", default=DEFAULT_MAPPING_PATH, help="Path to primary-to-dr.yaml")
    parser.add_argument("--direction", required=True, choices=["primary-to-dr", "dr-to-primary"])
    parser.add_argument("--connectors", default="ALL", help="Comma-separated connector names, or ALL")

    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--backup-only", action="store_true")
    parser.add_argument("--diff-only", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--commit-audit", action="store_true")
    parser.add_argument("--push", action="store_true", help="git push after --commit-audit")

    parser.add_argument("--verify-timeout", type=int, default=120)
    parser.add_argument("--verify-interval", type=int, default=5)

    args = parser.parse_args()

    mapping = common.parse_mapping_yaml(args.mapping)
    entries = filter_connectors(flatten_entries(mapping), args.connectors)

    if not entries:
        print("No matching connectors found in mapping file — nothing to do.")
        sys.exit(1)

    # --diff-only and --commit-audit don't need a live connection
    if args.diff_only:
        cmd_diff(entries, args.direction)
        return
    if args.commit_audit:
        cmd_commit_audit(args.direction, push=args.push)
        return

    conn_cfg = common.load_connection_config(args.config)
    opener = common.build_opener(conn_cfg["insecure"])
    connect_url = conn_cfg["connect_url"].rstrip("/")

    print(f"Kafka Connect URL: {connect_url}  (insecure={conn_cfg['insecure']})\n")

    if args.validate_only:
        cmd_validate(entries, args.direction, opener, conn_cfg, connect_url)
    elif args.backup_only:
        cmd_backup(entries, opener, conn_cfg, connect_url, args.direction)
    elif args.apply:
        cmd_apply(entries, args.direction, opener, conn_cfg, connect_url)
    elif args.verify:
        cmd_verify(entries, opener, conn_cfg, connect_url, args.verify_timeout, args.verify_interval)
    else:
        print("No action flag given. Use one of: --validate-only --backup-only --diff-only "
              "--apply --verify --commit-audit", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
