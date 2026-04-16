# graphmana setup-neo4j

## Synopsis

```
Usage: graphmana setup-neo4j [OPTIONS]
```

## Description

Download and configure Neo4j for user-space operation. Automatically deploys
the bundled GraphMana procedures JAR to the Neo4j plugins directory. Creates
a persistent config file at `~/.graphmana/config.yaml` so subsequent commands
do not need `--neo4j-home` or `--neo4j-password` on every invocation.

## Options

```
  --install-dir DIRECTORY     Directory to install Neo4j into.  [required]
  --password TEXT              Neo4j password for the 'neo4j' user.  [prompted]
  --version TEXT               Neo4j Community version to download.  [default: 5.26.2]
  --data-dir DIRECTORY         Custom data directory (use local SSD/scratch on clusters).
  --memory-auto                Auto-set heap and page cache based on available RAM.
  --install-java               Download Eclipse Temurin JDK 21 to user space (no admin needed).
  --skip-download              Use existing Neo4j at --install-dir (skip download).
  --neo4j-tarball FILE         Path to neo4j-community-5.26.x-unix.tar.gz for offline install.
  --deploy-plugin FILE         Path to a local graphmana-procedures.jar (skip bundled JAR).
  --bolt-port INTEGER          Bolt protocol port.  [default: 7687]
  --http-port INTEGER          HTTP browser port.  [default: 7474]
  --adopt                      Adopt a running Neo4j instance (deploys plugin, restarts).
  --i-understand-this-restarts-neo4j  Confirm restart for --adopt (skips interactive prompt).
  --verbose / --quiet          Verbose logging.
  --help                       Show this message and exit.
```

## Examples

```bash
# Standard installation with auto-memory
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto --password mypassword

# Offline install from a pre-downloaded tarball (e.g., from Zenodo)
graphmana setup-neo4j --install-dir ~/neo4j \
    --neo4j-tarball neo4j-community-5.26.0-unix.tar.gz \
    --password mypassword

# Use an existing Neo4j without downloading
graphmana setup-neo4j --install-dir ~/neo4j --skip-download --password mypassword

# Install alongside another Neo4j on different ports
graphmana setup-neo4j --install-dir ~/graphmana-neo4j \
    --bolt-port 7688 --http-port 7475 --password mypassword

# Adopt a running user-owned Neo4j instance
graphmana setup-neo4j --install-dir ~/neo4j --adopt --password mypassword
```

## Notes

- The `--password` flag is prompted interactively with confirmation if not
  passed on the command line. The password is stored in
  `~/.graphmana/config.yaml` (chmod 0600) so subsequent commands can
  authenticate automatically.
- The `--neo4j-tarball` flag validates the filename against the pattern
  `neo4j-community-5.26.x-unix.tar.gz`. The tarball is available from the
  [GraphMana Zenodo deposit](https://doi.org/10.5281/zenodo.19472835)
  or from `https://dist.neo4j.org/`.
- If the requested bolt port is already in use, setup refuses to proceed and
  prints instructions for resolving the conflict (kill process, change port,
  or adopt).
- The `--adopt` flag is intended for user-owned, single-user Neo4j
  installations only. It stops and restarts the instance to load the
  GraphMana procedures plugin.
- After setup, run `graphmana doctor` to verify the installation.

## See Also

- `graphmana neo4j-start` — start the configured Neo4j instance
- `graphmana neo4j-stop` — stop the instance
- `graphmana doctor` — run diagnostic health checks
- `graphmana check-filesystem` — verify data directory is on local storage
- [Offline install guide](../INSTALL.md#offline--air-gapped-install)
