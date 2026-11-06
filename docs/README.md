<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# VGI for DuckDB

VGI lets DuckDB call table, scalar, aggregate, table-in-out and `COPY`
functions served by remote workers over Apache Arrow IPC.

**This branch is the DuckDB community-repository placeholder, not the
extension.** VGI is distributed from the Query.Farm extension repository.
Loading this build tells you how to install VGI from there, or, once you have,
loads that build.

## Installation

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
```

## Documentation

**[https://query.farm/products/extensions/vgi](https://query.farm/products/extensions/vgi)**

The extension source is on the `v2.0` branch of
[Query-farm/vgi](https://github.com/Query-farm/vgi).
