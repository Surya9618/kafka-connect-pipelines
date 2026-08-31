# Kafka Connect DR Switch Pipelines — Deployment Guide

Two Jenkins pipelines (`switchover`, `switchback`) that flip JDBC `connection.url`
and IBM MQ `mq.hostname` values between primary and DR hosts, driven by a
mapping file in Git and executed via the Kafka Connect REST API.

This guide covers deploying the existing, tested pipeline into a **new
environment** (new Git host, new/secured Kafka Connect cluster).

---

## Repo layout

```
kafka-connect-pipelines/
├── Jenkinsfile.switchover
├── Jenkinsfile.switchback
├── vars/
│   └── drSwitchPipeline.groovy      # shared pipeline logic, both directions
├── scripts/
│   ├── dr_common.py                 # shared HTTP/YAML helpers (stdlib only)
│   ├── apply_config.py              # validate/backup/diff/apply/verify/audit
│   └── generate_dr_mapping.py       # pulls live configs, pre-fills mapping
├── connect_config.yaml              # non-secret connection settings
├── primary-to-dr.yaml               # the DR mapping (connector list + urls)
└── .gitignore
```

---

## Step 1 — Push the repo to the new Git host

```bash
cd ~/kafka-connect-pipelines
git remote add gitlab https://your-gitlab-host/your-namespace/kafka-connect-pipelines.git
git push gitlab main
```

In GitLab: create a **Project Access Token** (or Personal Access Token) with
`write_repository` scope. You'll use this as the password half of a Jenkins
credential in Step 3.

---

## Step 2 — Update `connect_config.yaml` for the new cluster

```bash
cat > connect_config.yaml << 'EOF'
# connect_config.yaml
# Non-secret connection settings. user/password stay blank here —
# Jenkins injects them at runtime via --user/--password, never committed.

connect_url: https://your-connect-cluster-hostname:8083

user:
password:

# Set true if the cluster uses a self-signed / internal CA cert.
insecure: true

timeout: 15

headers: {}
EOF
```

Replace `your-connect-cluster-hostname:8083` with the real endpoint.

```bash
git add connect_config.yaml
git commit -m "Point connect_config.yaml at new cluster"
git push
```

---

## Step 3 — Add two credentials in Jenkins

**Manage Jenkins → Credentials → Add Credentials**, twice:

| Field | Credential #1 | Credential #2 |
|---|---|---|
| Kind | Username with password | Username with password |
| Username | Connect cluster's basic-auth user | your GitLab username (or `oauth2`) |
| Password | Connect cluster's basic-auth password | GitLab access token |
| ID | `kafka-connect-rest-creds` | `gitlab-push-creds` |

Both IDs must match **exactly** — they're referenced by string in
`vars/drSwitchPipeline.groovy`.

> If your Kafka Connect cluster has **no auth** (e.g. an internal test
> instance), skip credential #1 — leave `user`/`password` blank in
> `connect_config.yaml` and the pipeline handles it gracefully.

---

## Step 4 — Repoint the Jenkins Shared Library

**Manage Jenkins → System → Global Trusted Pipeline Libraries** → edit the
`kafka-dr-lib` entry:

| Field | Value |
|---|---|
| Name | `kafka-dr-lib` (unchanged) |
| Default version | `main` |
| Retrieval method | Modern SCM → Git |
| Project Repository | your new GitLab repo URL |
| Credentials | `gitlab-push-creds` |

---

## Step 5 — Repoint both Jenkins jobs

For **both** `kafka-connectors-dr-switchover` and `kafka-connectors-dr-switchback`,
open the job → Configure → Pipeline section:

| Field | Value |
|---|---|
| Definition | Pipeline script from SCM |
| SCM | Git |
| Repository URL | your new GitLab repo URL |
| Credentials | `gitlab-push-creds` |
| Branch Specifier | `*/main` |
| Script Path | `Jenkinsfile.switchover` (or `.switchback`) |

---

## Step 6 — Populate `primary-to-dr.yaml` for real connectors

**Option A — regenerate from the live cluster** (pre-fills `primary_url`,
leaves `dr_url` blank for you to fill in):

```bash
cd scripts
python3 generate_dr_mapping.py \
  --connect-url https://your-connect-cluster-hostname:8083 \
  --user your-connect-user \
  --password your-connect-password \
  --insecure \
  --output ../primary-to-dr.yaml
cd ..
```

**Option B — hand-write it** following the schema:

```yaml
version: 1
direction: primary-to-dr

connectors:
  jdbc:
    - name: "your-connector-name"
      config_key: "connection.url"
      primary_url: "jdbc:oracle:thin:@primary-host:1521/svc"
      dr_url: "jdbc:oracle:thin:@dr-host:1521/svc"
      groups: ["retail"]          # optional

  mq:
    - name: "your-mq-connector"
      config_key: "mq.hostname"
      primary_url: "primary-mq.company.com"
      dr_url: "dr-mq.company.com"
```

> ⚠️ Regenerating via `generate_dr_mapping.py` **overwrites the whole file**,
> including any `dr_url` values and `groups` tags you've already filled in
> manually. If you've hand-edited the file, back it up before regenerating.

```bash
git add primary-to-dr.yaml
git commit -m "Populate DR mapping for new environment"
git push
```

---

## Step 7 — First run: dry-run only

**Jenkins → `kafka-connectors-dr-switchover` → Build with Parameters**

| Parameter | Value |
|---|---|
| CONNECTOR_LIST | `ALL` |
| GROUPS | *(leave blank)* |
| DRY_RUN | ✅ checked |

Confirms Checkout → Validate → Backup → Diff all work against the real,
secured cluster — no approval gate, no Apply, no Verify, no git push.

---

## Step 8 — Real run, then switchback

Uncheck `DRY_RUN`, click **Build**, approve when prompted, and confirm:
- **Apply** succeeds
- **Verify** reports `RUNNING`
- **Commit audit trail** pushes to GitLab (check the repo's `backups/` folder)

Then repeat the same on `kafka-connectors-dr-switchback` to confirm the
reverse direction.

---

## Reference: apply_config.py flags

```bash
python3 scripts/apply_config.py --direction primary-to-dr --validate-only \
  --connectors ALL --groups ""

python3 scripts/apply_config.py --direction primary-to-dr --backup-only

python3 scripts/apply_config.py --direction primary-to-dr --diff-only

python3 scripts/apply_config.py --direction primary-to-dr --apply

python3 scripts/apply_config.py --direction primary-to-dr --verify

python3 scripts/apply_config.py --direction primary-to-dr --commit-audit --push
```

Connection overrides (used automatically by Jenkins via credentials, but
available for manual/local testing too):

```bash
python3 scripts/apply_config.py --direction primary-to-dr --validate-only \
  --user someuser --password somepass --connect-url https://host:8083 --insecure
```

Selecting a subset of connectors:

```bash
# by name
--connectors jdbc-source-orders,mq-sink-payments

# by group (from the groups: field in primary-to-dr.yaml)
--connectors ALL --groups retail

# union of both
--connectors mq-source-alerts --groups warehouse
```

---

## Known limitations (carried over from development)

- `generate_dr_mapping.py` overwrites `dr_url` and `groups` fields on
  regeneration — these are hand-maintained, so back up before regenerating.
- `groups` is optional per connector; a run with `--groups <name>` that
  matches nothing will hard-fail with "No matching connectors found" rather
  than silently doing nothing.
- If GitLab is also self-managed with a self-signed cert, Jenkins git
  operations may need separate TLS trust configuration — this is different
  from the `insecure: true` flag, which only affects the Python script's
  connection to Kafka Connect.
