# AGENTS.md — scope: `modules/ignite-graph` only

Everything below is about `modules/ignite-graph` (a TinkerPop graph layer over Apache Ignite). Other modules in this repo are out of scope; general repo build/release info is in `DEVNOTES.md`.

## Build & verify

- **Not in the root reactor** — the root `pom.xml` never builds it. Build from repo root with:
  `mvn -f modules/ignite-graph package -DskipTests` (verified working; exit 0).
- Own standalone build: `groupId de.kp.works`, version `0.5.0`, Java 17 source/target. It does **not** inherit `parent-internal`, so the repo-wide `-Pcheckstyle` convention from `DEVNOTES.md` does not apply here.
- **Zero test sources** — `mvn test` proves nothing beyond compilation. Verification = clean compile/package.
- **Dependency trap**: it needs `org.apache.ignite:ignite-core` + `ignite-json` at `2.16.999-SNAPSHOT` (provided scope) but its pom declares no repositories. The WSL `~/.m2/settings.xml` mirrors *all* repos to aliyun (snapshots disabled), so Apache snapshot artifacts can never be downloaded — they must already exist in the local repo `/mnt/d/tools/maven/repository` (installed locally — `maven-metadata-local.xml` — from the sibling checkout `/mnt/d/Code/GIT/ignite`). If those SNAPSHOTs are missing or the sibling ignite code changed, rebuild/install the sibling checkout first. TinkerPop is pinned to `3.7.3`.

## Structure (3 modules, build order matters)

- `works-ignite-common` → Ignite access layer: table/cache admin, SQL query builders (`query/*`), mutations. No TinkerPop types.
- `works-ignite-graph` → TinkerPop `Graph` implementation. Entry point: `IgniteGraph.open(name, properties)` (Gremlin GraphFactory). Registered via `META-INF/services/org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin`.
- `works-ignite-gremlin-server` → Ignite plugin (`META-INF/services/org.apache.ignite.plugin.PluginProvider` → `GremlinServerPluginProvider`) that boots an embedded Gremlin Server inside an Ignite node.

## Runtime & config gotchas

- **Graph name = namespace = Ignite instance name.** All graphs share one Ignite node; physical caches/tables are prefixed `<namespace>_<name>` (e.g. `default_vertices`, `default_edges`).
- **Single-instance singleton**: `IgniteConnect.defaultIgnite` is static — the first started grid wins. A second graph's own Ignite config file effectively has no effect (also noted in the module README).
- Ignite config resolution: default file is `config/graph-config.xml` **relative to process CWD** (override with properties key `gremlin.graph.ignite.cfg`). Spring bean ids are tried in order `{name}`, `{name}.cfg`, `graph-{name}`, `default`, `` (empty) — none matching returns `null` and later ops fail with NPE-ish "use default ignite" logs.
- **Two storage models**: fixed Vertex/Edge tables (`<ns>_vertices`, `<ns>_edges`) vs. per-label *Document* tables created lazily (existence checked via `admin.hasCache(<ns>_<label>)`). `addVertex` routes to document storage when the `_class=document` property is set; labels `vertices`/`edges` are reserved.
- **Lazy init + hang risk**: tables are created on first model access, and `init()` busy-waits until the cluster is ACTIVE (log says "use control.sh"). With persistence enabled, activate the cluster first or graph operations spin forever.
- No transactions and no graph `variables()` — both throw (`Graph.Exceptions.transactionsNotSupported()` etc.).
- Element lookups go through JVM-local Caffeine caches (defaults: 1M entries / 60s TTL for vertices & edges, 1000/60s for vertex→edge relations) — stale/identity behavior across processes is expected, not a bug.
- Embedded Gremlin Server **auto-starts only when the Ignite instance name is exactly `graph`** (non-client), or when a `GremlinPluginConfiguration` is registered in `IgniteConfiguration`. Server files are read from `$IGNITE_HOME/config/gremlin-server/`: `gremlin-server-ignite.yaml` plus graph properties `ignite-<instanceName>.properties` (fallback `ignite-default.properties`) — this repo ships **none** of those for ignite-graph (a Gremlin-server yaml referencing this plugin exists only in `modules/ignite-janus/conf/gremlin-server/`, but it is janus-oriented).
- `works-ignite-gremlin-server`'s `target/libs/` is populated by maven-dependency-plugin at `package` (runtime deps for launching); `ignite-core` itself is `provided`, so it must come from `IGNITE_HOME`/the node's classpath.

## Query execution model (where current work happens)

- Traversal strategies `IgniteGraphStepStrategy` / `IgniteVertexStepStrategy` are registered in `IgniteGraph`'s static block; they rewrite `hasLabel`/`has`/range steps into Ignite **SQL** executed via `SqlFieldsQuery` (`works-ignite-common/.../query/IgniteQuery.java`).
- Recent fork commits (Chinese messages, actively developed by the fork owner) rework exactly this layer: `IgniteQuery`/`IgniteVertexQuery`/`IgniteDocumentQuery` (renamed from `IgniteSqlQuery`, `IgniteLabelQuery` deleted) and `IgniteGraphStep` pushdown. Expect this area to be in flux; changes here affect both `works-ignite-common` and `works-ignite-graph`.
- `de.kp.works.ignite.gremlin.sql` (`IgniteGraphTraversalSource`, `IgniteSqlStrategy`) is wired through the JSR-223 plugin, used by `GremlinPlugin.traversal()` in the server module.
