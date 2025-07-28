# audit-tool: Kubernetes Audit Log Analysis Tool

The `audit-tool` allows you to analyze and compare Kubernetes API server audit logs, etcd metrics, and alerts grabbed using the gather_for_tool script while running a must-gather, by importing them into Prometheus and enabling querying with PromQL. 

---

## Installation

### Build from Source

You can install `audit-tool` directly using `go install`:

```bash
go install [github.com/natamm4/audit-tool/cmd/audit-tool@latest](https://github.com/natamm4/audit-tool/cmd/audit-tool@latest)
```

---

## Usage

This guide outlines the steps to collect audit logs, etcd metrics, and alerts, process them and import them into Prometheus with audit-tool, and query them using PromQL.

#### 1. Change to your desired working directory:

   ```bash
   cd /wherever/you/want
   ```
#### 2. Gather Audit Logs, Etcd Metrics, and Alerts:

   ```bash
   oc adm must-gather -- /usr/bin/gather_for_tool
   ```

#### 3. Process and Import with audit-tool:

   Convert the collected audit logs into OpenMetrics format, create Prometheus TSDB blocks for each metrics file, and start a combined Prometheus instance.

   ```bash
   audit-tool visualize --read /path/to/must-gather
   ```

#### 4. Access Prometheus UI:

   Open your web browser and navigate to http://localhost:9090.

#### 5. Query your audit data!
   
---

## Queryable

Once Prometheus is running, you can use PromQL to analyze your audit logs. The following are the bits of info queryable for the audit events:

- verb (eg. 'update', 'get', etc.)
- resource (eg. 'pods')
- subresource
- name (eg. 'etcd-endpoints')
- namespace 
- user
- uid
- code (http status code eg. 200)
- stage (eg. 'RequestReceived', 'ResponseComplete')
- duration 

---

## Example Queries

#### To discover who was creating watches:

```bash
count(audit_event_timestamp{verb="watch"}) by (user)
```

#### To see etcd endpoint updates:

```bash
count(audit_event_timestamp{verb="update", resource="configmaps", name="etcd-endpoints"}) by (user)
```

#### To see who was listing pods frequently:

```bash
count(audit_event_timestamp{verb="list",resource="pods"}) by (user)
```
