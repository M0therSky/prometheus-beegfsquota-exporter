# prometheus-beegfs-quota-exporter

Prometheus exporter for BeeGFS group quota metrics.

**Compatible with BeeGFS >= 8.3.0**

## Metrics

| Metric | Type | Description |
|---|---|---|
| `beegfs_quota_space_used_bytes` | Gauge | Space currently used |
| `beegfs_quota_space_limit_bytes` | Gauge | Space quota limit |
| `beegfs_quota_inode_used_total` | Gauge | Inodes currently used |
| `beegfs_quota_inode_limit_total` | Gauge | Inode quota limit |
| `beegfs_quota_space_exceeded` | Gauge | 1 if space quota is at or above limit |
| `beegfs_quota_inode_exceeded` | Gauge | 1 if inode quota is at or above limit |
| `beegfs_quota_scrape_success` | Gauge | 1 if last scrape succeeded |
| `beegfs_quota_scrape_duration_seconds` | Gauge | Duration of last scrape |

All quota metrics carry `group`, `gid`, and `pool` labels.

## Usage

```
beegfs-quota-exporter [flags]

  -host string    Host address to listen on (default "localhost")
  -port int       Port to listen on (default 9742)
  -beegfs string  Path to the beegfs CLI binary (default "beegfs")
```

Metrics are exposed at `http://<host>:<port>/metrics`.

## Build

```sh
make build
```

## Requirements

- BeeGFS >= 8.3.0 (`beegfs quota list-usage` command must be available)
- `getent` for LDAP group name resolution
