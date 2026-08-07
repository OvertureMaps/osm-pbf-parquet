# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "xmltodict",
# ]
# ///

import xmltodict
import pandas as pd
from pathlib import Path
from datetime import datetime

OSM_XML_FILE = "test.osm"
PARQUET_DIR = "./parquet"

# Parse, process OSM XML file into a dataframe
xml_lines = Path(OSM_XML_FILE).read_text()
xml_dict = xmltodict.parse(xml_lines)["osm"]
xml_rows = []
for feature_type in ["node", "way", "relation"]:
    for row in xml_dict[feature_type]:
        new_entries = {}
        new_entries["type"] = feature_type
        for key, value in row.items():
            new_value = value
            # Everything is read in as strings, try to parse
            try:
                new_value = int(value)
            except Exception:
                try:
                    new_value = float(value)
                except Exception:
                    pass
            if "timestamp" in key:
                new_value = round(datetime.fromisoformat(value).timestamp() * 1e3)

            if "@" in key:
                new_entries[key.replace("@", "")] = new_value
            else:
                new_entries[key] = new_value

        xml_rows.append(new_entries)
xml_df = pd.DataFrame.from_dict(xml_rows)
xml_df = xml_df.replace({float("nan"): None})

# Read in parquet data
pq_df = pd.read_parquet(PARQUET_DIR)
# Parquet timestamps read back as datetime64; convert to epoch milliseconds
# to match the XML conversion above (comparing datetime64 against int with
# .eq() is always False, which the old any-column-matches check hid)
pq_ts = pq_df["timestamp"].astype("datetime64[ms]")
pq_df["timestamp"] = (
    pd.Series(pq_ts.to_numpy().astype("int64"), index=pq_df.index)
    .astype("Int64")
    .mask(pq_ts.isna())
)

print("Samples, metadata")
print(xml_df.head())
print(pq_df.head())
print(xml_df.dtypes)
print(pq_df.dtypes)

# Check for duplicates
duplicate_count = pq_df.duplicated(subset=["id", "type"]).sum()
if duplicate_count > 0:
    print(xml_df.count())
    print(pq_df.count())
    print(xml_df.groupby("type").count())
    print(pq_df.groupby("type").count())
    raise AssertionError(f"Duplicate count: {duplicate_count}")

# Find all missing from either side
joined = xml_df.set_index(["id", "type"]).join(
    pq_df.set_index(["id", "type"]),
    on=["id", "type"],
    how="outer",
    rsuffix="_pq",
    validate="one_to_one",
)

print("Checking parquet has all XML data")
pq_missing = joined[joined["version_pq"].isnull()]
if pq_missing.count().sum() > 0:
    print(pq_missing.count())
    print(pq_missing.head())
    raise AssertionError(
        f"Unexpected missing rows from parquet, count: {pq_missing.count().sum()}"
    )

print("Checking unexpected parquet data (not in XML)")
xml_missing = joined[joined["version"].isnull()]
if xml_missing.count().sum() > 0:
    print(xml_missing.count())
    print(xml_missing.head())
    raise AssertionError(
        f"Unexpected extra rows in parquet, count: {xml_missing.count().sum()}"
    )

print("Checking mismatched scalar values")


def matches(left, right):
    # Null-safe equality: NaN != NaN in pandas, but a value missing from
    # both sides (e.g. lat/lon on ways) is a match, not a mismatch. With
    # nullable dtypes (Int64) eq() yields pd.NA for one-sided nulls; those
    # are genuine mismatches, so fill False
    both_na = left.isna() & right.isna()
    return (left.eq(right) | both_na).fillna(False).astype(bool)


def matches_close(left, right, tolerance=1e-9):
    both_na = left.isna() & right.isna()
    return ((left - right).abs().lt(tolerance) | both_na).fillna(False).astype(bool)


# Every column must match; each is checked independently so a wrong
# timestamp can't hide behind a correct version (or vice versa)
scalar_checks = {
    "version": matches(joined["version"], joined["version_pq"]),
    "timestamp": matches(joined["timestamp"], joined["timestamp_pq"]),
    "lat": matches_close(joined["lat"], joined["lat_pq"]),
    "lon": matches_close(joined["lon"], joined["lon_pq"]),
}
# Geofabrik public extracts strip user metadata, so the XML source may
# legitimately lack these columns — but the parquet schema always carries
# them, so their absence from the output is a bug, never a skip
for optional_column in ["uid", "user", "changeset"]:
    assert (
        optional_column in pq_df.columns
    ), f"parquet output missing '{optional_column}' column"
    if optional_column in xml_df.columns:
        scalar_checks[optional_column] = matches(
            joined[optional_column], joined[f"{optional_column}_pq"]
        )
    else:
        print(f"Column '{optional_column}': not in source file, skipping")
mismatch_count = 0
for column, column_matches in scalar_checks.items():
    bad = joined[~column_matches]
    if len(bad) > 0:
        mismatch_count += len(bad)
        print(f"Column '{column}': {len(bad)} mismatched rows")
        print(bad[[column, f"{column}_pq"]].head())
if mismatch_count > 0:
    raise AssertionError(f"Mismatched data, count: {mismatch_count}")


def remap_tags(tags):
    if not tags:
        return []
    if type(tags) == dict:
        return [(tags["@k"], tags["@v"])]
    return [(pair["@k"], pair["@v"]) for pair in tags]


print("Checking tags")
mismatched_tags = joined[
    ~(joined["tag"].apply(lambda tags: remap_tags(tags)).eq(joined["tags"]))
]
if mismatched_tags.count().sum() > 0:
    print(mismatched_tags.count())
    print(mismatched_tags[["tag", "tags"]].head())
    raise AssertionError(f"Mismatched tags, count: {mismatched_tags.count().sum()}")


def remap_nodes(nodes):
    if nodes is None or len(nodes) == 0:
        return []
    if type(nodes) == dict:
        return [int(nodes.get("@ref", nodes.get("ref")))]
    return [int(node.get("@ref", node.get("ref"))) for node in nodes]


def compare_nodes(xml_nodes, pq_nodes):
    for xml, pq in zip(xml_nodes, pq_nodes):
        if str(xml) != str(pq):
            return False
    return True


print("Checking nodes")
joined["nd_remap"] = joined["nd"].apply(lambda nodes: remap_nodes(nodes))
joined["nds_remap"] = joined["nds"].apply(lambda nodes: remap_nodes(nodes))
joined["nd_compare"] = joined.apply(
    lambda row: compare_nodes(row["nd_remap"], row["nds_remap"]), axis=1
)
mismatched_nodes = joined[~(joined["nd_compare"])]
if mismatched_nodes.count().sum() > 0:
    print(mismatched_nodes.count())
    print(mismatched_nodes[["nd_remap", "nds_remap"]].head())
    raise AssertionError(f"Mismatched nodes, count: {mismatched_nodes.count().sum()}")


def remap_members(members):
    if members is None or len(members) == 0:
        return []
    if type(members) == dict:
        return [
            (
                members.get("@type", members.get("type")),
                int(members.get("@ref", members.get("ref"))),
                members.get("@role", members.get("role")),
            )
        ]
    return [
        (
            member.get("@type", member.get("type")),
            int(member.get("@ref", member.get("ref"))),
            member.get("@role", member.get("role")),
        )
        for member in members
    ]


def compare_members(xml_members, pq_members):
    for xml, pq in zip(xml_members, pq_members):
        if xml != pq:
            return False
    return True


print("Checking members")
joined["member_remap"] = joined["member"].apply(lambda nodes: remap_members(nodes))
joined["members_remap"] = joined["members"].apply(lambda nodes: remap_members(nodes))
joined["members_compare"] = joined.apply(
    lambda row: compare_members(row["member_remap"], row["members_remap"]), axis=1
)
mismatched_members = joined[~(joined["members_compare"])]
if mismatched_members.count().sum() > 0:
    print(mismatched_members.count())
    print(mismatched_members[["member_remap", "members_remap"]].head())
    raise AssertionError(
        f"Mismatched members, count: {mismatched_members.count().sum()}"
    )
