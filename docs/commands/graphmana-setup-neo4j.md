# graphmana setup-neo4j

## Synopsis

```
Usage: graphmana setup-neo4j [OPTIONS]
```

## Help Output

```
Usage: graphmana setup-neo4j [OPTIONS]

  Download and configure Neo4j for user-space operation.

Options:
  --install-dir DIRECTORY  Directory to install Neo4j into.  [required]
  --version TEXT           Neo4j Community version to download.
  --data-dir DIRECTORY     Custom data directory (use local SSD/scratch on
                           clusters).
  --memory-auto            Auto-set heap and page cache based on available
                           RAM.
  --install-java           Download Eclipse Temurin JDK 21 to user space
                           (no admin privileges needed).
  --verbose / --quiet      Verbose logging.
  --help                   Show this message and exit.
```

## Notes

- The bundled GraphMana procedures JAR is automatically deployed to the
  Neo4j `plugins/` directory during setup.
- Use `--install-java` on systems where Java 21+ is not available and you
  cannot use `sudo` or `module load` (common on HPC clusters).
- The `--memory-auto` flag reads available RAM and sets Neo4j heap and
  page cache to appropriate values.
