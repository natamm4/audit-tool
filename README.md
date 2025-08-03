# audit-tool: Kubernetes Audit Log Analysis Tool

The `audit-tool` allows you to analyze and compare Kubernetes API server audit logs, etcd metrics, and alerts grabbed using the gather_for_visualization must-gather script, by importing them into Prometheus and enabling querying with PromQL. 

---

## Installation

### Build from Source

You can install `audit-tool` directly using `go install`:

```bash
go install [github.com/natamm4/audit-tool/cmd/audit-tool@latest](https://github.com/natamm4/audit-tool/cmd/audit-tool@latest)
```

---

## Usage

This guide outlines the steps to collect the audit logs, etcd and alert metrics, the process and and import them into Prometheus with audit-tool, and finally query them using PromQL.

#### 1. Gather Audit Logs, Etcd Metrics, and Alerts:

   ```bash
   oc adm must-gather -- /usr/bin/gather_for_visualization
   ```

#### 2. Process and Import with audit-tool:

   Convert the collected audit logs into OpenMetrics format, create Prometheus TSDB blocks for each metrics file, and start a combined Prometheus instance.

   ```bash
   audit-tool visualize --read /PATH/TO/must-gather
   ```

#### 3. Access Prometheus UI:

   Open your web browser and navigate to http://localhost:9090.

#### 4. Query away!

   Use PromQL to perform RCA.
   
---

## Queryable

Once Prometheus is running, you can use PromQL to analyze your metrics. The following are the bits of info queryable for the audit events:

- verb (eg. 'update', 'get', etc.)
- resource (eg. 'pods')
- subresource
- name (eg. 'etcd-endpoints')
- namespace 
- user
- code (http status code eg. 200)
- stage (eg. 'RequestReceived', 'ResponseComplete')

For the etcd metrics:

- container
- endpoint
- instance
- job
- namespace
- pod
- service

For the alerts:

- alertname
- alertstate
- container
- endpoint
- instance
- job
- namespace
- pod
- service
- severity
- type

---

## Example Queries

#### To discover who was creating watches:

```bash
count(audit_event_duration_seconds_bucket{verb="watch"}) by (user)
```

#### To see etcd endpoint updates:

```bash
count(audit_event_duration_seconds_bucket{verb="update", resource="configmaps", name="etcd-endpoints"}) by (user)
```

#### To see who was listing pods frequently:

```bash
count(audit_event_duration_seconds_bucket{verb="list",resource="pods"}) by (user)
```

---

## Future Improvements

Currently, the gather_for_visualization script grabs etcd metrics and alerts (because the builder was on the etcd team), but this can be expanded to include gather more metrics.
