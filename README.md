# audit-tool: Kubernetes Audit Log Analysis Tool

The `audit-tool` allows you to analyze Kubernetes API server audit logs by converting them into a Prometheus-compatible format, enabling querying with PromQL.

---

## Installation

### Build from Source

You can install `audit-tool` directly using `go install`:

```bash
go install [github.com/natamm4/audit-tool/cmd/audit-tool@latest](https://github.com/natamm4/audit-tool/cmd/audit-tool@latest)
```

---

## Usage

This guide outlines the steps to collect audit logs, process them with audit-tool, and query them using Prometheus.

1. Change to your desired working directory:

   ```bash
   cd /wherever/you/want
   ```
2. Gather Audit Logs:

   ```bash
   oc adm must-gather -- /usr/bin/gather_audit_logs
   ```

3. Process Audit Logs with audit-tool:

   Convert the collected audit logs into OpenMetrics format.

   ```bash
      audit-tool query --dir /path/to/audit_logs/kube-apiserver --output openmetricsTime > auditlogs.openmetrics
   ```

4. Create Prometheus TSDB Blocks:

   Use promtool to create Prometheus TSDB blocks from the OpenMetrics output.

   ```bash
   promtool tsdb create-blocks-from openmetrics auditlogs.openmetrics prom-blocks/
   ```

5. Start Prometheus:

   Launch a Prometheus instance, pointing it to the newly created TSDB blocks.

   ```bash
   prometheus --storage.tsdb.path=/path/to/prom-blocks/
   ```

6. Access Prometheus UI:

   Open your web browser and navigate to http://localhost:9090.

7. Query your audit data!
   
---

## Usage With Extra Must-Gather Metrics

You can also gather additional metrics for more comprehensive analysis:

1. Gather Audit Logs and Additional Metrics:

   ```bash
   oc adm must-gather -- "/usr/bin/gather_audit_logs && /usr/bin/gather_etcd_more"
   ```

2. Decompress the metrics.openmetrics must-gather file:

   ```bash
   gunzip -k /path/to/etcd_info/metrics.openmetrics.gz
   ```

3. Create Prometheus TSDB Blocks from decompressed metrics.openmetrics file:

   Use promtool to create Prometheus TSDB blocks from this file.

   ```bash
   promtool tsdb create-blocks-from openmetrics /path/to/must-gather/metrics.openmetrics prom-blocks/
   ```
   
4. Follow steps 3-7 from above, ensuring to create the auditlogs.openmetrics TSDB blocks in the same prom-blocks/ directory as where the TSDB blocks for the metrics.openmetrics were made.

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
