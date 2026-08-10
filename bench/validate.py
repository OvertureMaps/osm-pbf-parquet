#!/usr/bin/env -S uv run --quiet --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "pyarrow"]
# ///
"""Medium-weight accuracy comparison across OSM transcode tool outputs.

Computes per-element-type aggregates (counts, id sums, tag/ref/member
aggregates) for each tool's output, normalizing over schema differences,
and diffs everything against a reference tool (default: osm-pbf-parquet).

This is aggregate-level validation: it will catch missing/extra/corrupted
rows and values with very high probability, but it is not a row-by-row
join. Schema gaps (columns a tool simply does not produce) are reported
as N/A rather than mismatches — e.g. DuckDB's ST_ReadOSM omits changeset,
timestamp, uid, user, version and visible.
"""
import argparse
import glob
import os
import sys

import duckdb

# Metrics per type. Each tool adapter returns {metric_name: value | None}.
NODE_METRICS = ["count", "sum_id", "tag_entries", "sum_uid", "sum_changeset"]
WAY_METRICS = ["count", "sum_id", "tag_entries", "refs_count", "refs_sum", "sum_uid", "sum_changeset"]
REL_METRICS = ["count", "sum_id", "tag_entries", "members_count", "members_ref_sum", "roles_len_sum", "sum_uid", "sum_changeset"]


def q1(con, sql):
    return con.execute(sql).fetchone()


def read_osm_pbf_parquet(con, base):
    """Layout: type=node|way|relation partitions, full schema."""
    out = {}
    for t in ["node", "way", "relation"]:
        g = os.path.join(base, f"type={t}", "*.parquet")
        if not glob.glob(g):
            out[t] = None
            continue
        if t == "node":
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(uid), sum(changeset)
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(NODE_METRICS, r))
        elif t == "way":
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(nds)),
                                   sum(list_aggregate(list_transform(nds, x -> x.ref), 'sum')),
                                   sum(uid), sum(changeset)
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(WAY_METRICS, r))
        else:
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(members)),
                                   sum(list_aggregate(list_transform(members, m -> m.ref), 'sum')),
                                   sum(list_aggregate(list_transform(members, m -> length(coalesce(m.role,''))), 'sum')),
                                   sum(uid), sum(changeset)
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(REL_METRICS, r))
    return out


def read_duckdb(con, base):
    """Layout: kind=node|way|relation partitions from ST_ReadOSM.
    No metadata columns; ways and relations both use refs/ref_roles."""
    out = {}
    for t, kind in [("node", "node"), ("way", "way"), ("relation", "relation")]:
        g = os.path.join(base, f"kind={kind}", "*.parquet")
        if not glob.glob(g):
            out[t] = None
            continue
        if t == "node":
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags))
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(NODE_METRICS, list(r) + [None, None]))
        elif t == "way":
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(refs)),
                                   sum(list_aggregate(refs, 'sum'))
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(WAY_METRICS, list(r) + [None, None]))
        else:
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(refs)),
                                   sum(list_aggregate(refs, 'sum')),
                                   sum(list_aggregate(list_transform(ref_roles, x -> length(coalesce(x,''))), 'sum'))
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(REL_METRICS, list(r) + [None, None]))
    return out


def read_osm_parquetizer(con, base):
    """Layout: input.osm.pbf.node.parquet / .way.parquet / .relation.parquet.
    Schema (osm-parquetizer): nodes(id, version, timestamp, changeset, uid,
    user_sid, tags list<struct<key,value>>, latitude, longitude), ways(...,
    nodes list<struct<index,nodeId>>), relations(..., members
    list<struct<id,role,type>>)."""
    out = {}
    def find(suffix):
        m = glob.glob(os.path.join(base, f"*.{suffix}.parquet"))
        return m[0] if m else None

    f = find("node")
    if f:
        r = q1(con, f"""SELECT count(*), sum(id), sum(len(tags)), sum(uid), sum(changeset)
                        FROM read_parquet('{f}')""")
        out["node"] = dict(zip(NODE_METRICS, r))
    else:
        out["node"] = None
    f = find("way")
    if f:
        r = q1(con, f"""SELECT count(*), sum(id), sum(len(tags)), sum(len(nodes)),
                               sum(list_aggregate(list_transform(nodes, x -> x.nodeId), 'sum')),
                               sum(uid), sum(changeset)
                        FROM read_parquet('{f}')""")
        out["way"] = dict(zip(WAY_METRICS, r))
    else:
        out["way"] = None
    f = find("relation")
    if f:
        r = q1(con, f"""SELECT count(*), sum(id), sum(len(tags)), sum(len(members)),
                               sum(list_aggregate(list_transform(members, m -> m.id), 'sum')),
                               sum(list_aggregate(list_transform(members, m -> length(coalesce(m.role,''))), 'sum')),
                               sum(uid), sum(changeset)
                        FROM read_parquet('{f}')""")
        out["relation"] = dict(zip(REL_METRICS, r))
    else:
        out["relation"] = None
    return out


def read_osm2orc(con, base):
    """Layout: single .orc file, osm2orc schema: type partition column absent;
    columns include id, type ('node'/'way'/'relation'), tags map, nds
    array<struct<ref>>, members array<struct<type,ref,role>>, changeset,
    timestamp, uid, user, version, visible. Read via pyarrow (duckdb has no
    ORC reader) and aggregate in duckdb over arrow batches."""
    import pyarrow.orc as orc  # noqa: deferred heavy import

    files = glob.glob(os.path.join(base, "*.orc"))
    if not files:
        return {"node": None, "way": None, "relation": None}
    t = orc.ORCFile(files[0]).read()
    con.register("orc_tbl", t)
    out = {}
    r = q1(con, """SELECT count(*), sum(id), sum(cardinality(tags)), sum(uid), sum(changeset)
                   FROM orc_tbl WHERE type='node'""")
    out["node"] = dict(zip(NODE_METRICS, r))
    r = q1(con, """SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(nds)),
                          sum(list_aggregate(list_transform(nds, x -> x.ref), 'sum')),
                          sum(uid), sum(changeset)
                   FROM orc_tbl WHERE type='way'""")
    out["way"] = dict(zip(WAY_METRICS, r))
    r = q1(con, """SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(members)),
                          sum(list_aggregate(list_transform(members, m -> m.ref), 'sum')),
                          sum(list_aggregate(list_transform(members, m -> length(coalesce(m.role,''))), 'sum')),
                          sum(uid), sum(changeset)
                   FROM orc_tbl WHERE type='relation'""")
    out["relation"] = dict(zip(REL_METRICS, r))
    con.unregister("orc_tbl")
    return out


def read_sedona(con, base):
    """Layout: kind=node|way|relation partitions (Spark partitionBy), full
    metadata schema. Ways and relations both use refs; roles in ref_roles."""
    out = {}
    for t in ["node", "way", "relation"]:
        g = os.path.join(base, f"kind={t}", "*.parquet")
        if not glob.glob(g):
            out[t] = None
            continue
        if t == "node":
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(uid), sum(changeset)
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(NODE_METRICS, r))
        elif t == "way":
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(refs)),
                                   sum(list_aggregate(refs, 'sum')), sum(uid), sum(changeset)
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(WAY_METRICS, r))
        else:
            r = q1(con, f"""SELECT count(*), sum(id), sum(cardinality(tags)), sum(len(refs)),
                                   sum(list_aggregate(refs, 'sum')),
                                   sum(list_aggregate(list_transform(ref_roles, x -> length(coalesce(x,''))), 'sum')),
                                   sum(uid), sum(changeset)
                            FROM read_parquet('{g}')""")
            out[t] = dict(zip(REL_METRICS, r))
    return out


ADAPTERS = {
    "osm-pbf-parquet": read_osm_pbf_parquet,
    "duckdb": read_duckdb,
    "osm-parquetizer": read_osm_parquetizer,
    "osm2orc": read_osm2orc,
    "sedona": read_sedona,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="dir containing per-tool output subdirs")
    ap.add_argument("--tools", default="osm-pbf-parquet,duckdb,osm-parquetizer,osm2orc")
    ap.add_argument("--reference", default="osm-pbf-parquet")
    ap.add_argument("--threads", type=int, default=6)
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute(f"SET threads={args.threads}")

    tools = [t for t in args.tools.split(",") if t]
    results = {}
    for tool in tools:
        base = os.path.join(args.base, tool)
        if not os.path.isdir(base):
            print(f"note: no output dir for {tool}, skipping")
            continue
        try:
            results[tool] = ADAPTERS[tool](con, base)
        except Exception as e:  # surface but keep comparing other tools
            print(f"ERROR reading {tool}: {e}")

    ref = args.reference
    if ref not in results:
        print(f"reference tool {ref} has no results; aborting comparison")
        sys.exit(1)

    exit_code = 0
    for t, metrics in [("node", NODE_METRICS), ("way", WAY_METRICS), ("relation", REL_METRICS)]:
        print(f"\n== {t} ==")
        header = ["metric", *results.keys()]
        rows = []
        for m in metrics:
            row = [m]
            ref_v = (results[ref].get(t) or {}).get(m)
            for tool in results:
                v = (results[tool].get(t) or {}).get(m)
                if v is None:
                    row.append("N/A")
                elif tool == ref:
                    row.append(str(v))
                elif ref_v is None:
                    # reference lacks this metric; report, don't fail
                    row.append(f"no-ref({v})")
                elif v == ref_v:
                    row.append("match")
                else:
                    row.append(f"MISMATCH({v})")
                    exit_code = 1
            rows.append(row)
        widths = [max(len(r[i]) for r in [header] + rows) for i in range(len(header))]
        for r in [header] + rows:
            print("  ".join(c.ljust(w) for c, w in zip(r, widths)))

    print("\nSchema coverage notes:")
    print("- duckdb (ST_ReadOSM): no changeset/timestamp/uid/user/version/visible; member types via ref_types")
    print("- osm-parquetizer: user as dictionary id (user_sid), lat/lon as latitude/longitude; no visible")
    print("- osm2orc: full schema in a single ORC file")
    print("- sedona (osmpbf source): full metadata schema; location struct for lat/lon; member types via ref_types")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
