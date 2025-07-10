# audit-tool: Kubernetes Audit Log Analysis Tool

The `audit-tool` allows you to analyze Kubernetes API server audit logs by converting them into a Prometheus-compatible format, enabling powerful querying with PromQL.

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
      audit-tool query --dir /path/to/audit_logs/kube-apiserver --output openmetricsTime > metrics.out
   ```

4. Create Prometheus TSDB Blocks:

   Use promtool to create Prometheus TSDB blocks from the OpenMetrics output.

   ```bash
   promtool tsdb create-blocks-from openmetrics metrics.out prom-blocks/
   ```

5. Start Prometheus:

   Launch a Prometheus instance, pointing it to the newly created TSDB blocks.

   ```bash
   prometheus --storage.tsdb.path=/path/to/prom-blocks/
   ```

6. Access Prometheus UI:

   Open your web browser and navigate to http://localhost:9090.

7. Query your audit data!
   
