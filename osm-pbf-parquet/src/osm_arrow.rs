use std::fmt;
use std::sync::{Arc, LazyLock};

use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, MapBuilder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::ArrowError;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum OSMType {
    Node,
    Way,
    Relation,
}

impl OSMType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OSMType::Node => "node",
            OSMType::Way => "way",
            OSMType::Relation => "relation",
        }
    }
}

impl fmt::Display for OSMType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

static OSM_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(osm_arrow_schema()));

pub fn osm_arrow_schema() -> Schema {
    // Derived from this schema:
    // `id` BIGINT,
    // `tags` MAP<STRING, STRING>,
    // `lat` DOUBLE,
    // `lon` DOUBLE,
    // `nds` ARRAY<STRUCT<ref: BIGINT>>,
    // `members` ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
    // `changeset` BIGINT,
    // `timestamp` TIMESTAMP,
    // `uid` BIGINT,
    // `user` STRING,
    // `version` BIGINT,
    // `visible` BOOLEAN

    // TODO - add type field when not writing with partitions
    // `type` STRING
    // Field::new("type", DataType::Utf8, false)

    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
        Field::new(
            "nds",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new("ref", DataType::Int64, true)])),
                true,
            ))),
            true,
        ),
        Field::new(
            "members",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::Utf8, true),
                    Field::new("ref", DataType::Int64, true),
                    Field::new("role", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        ),
        Field::new("changeset", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("uid", DataType::Int32, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
        Field::new("visible", DataType::Boolean, true),
    ])
}

pub fn cached_osm_schema() -> Arc<Schema> {
    OSM_SCHEMA.clone()
}

pub struct OSMArrowBuilder {
    id_builder: Int64Builder,
    tags_builder: MapBuilder<StringBuilder, StringBuilder>,
    lat_builder: Float64Builder,
    lon_builder: Float64Builder,
    // `nds` and `members` list columns are built from typed child builders
    // plus manually-tracked offsets. This avoids the per-element dynamic
    // downcast that `ListBuilder<StructBuilder>::field_builder` performs.
    nodes_ref_builder: Int64Builder,
    nodes_offsets: Vec<i32>,
    members_type_builder: StringBuilder,
    members_ref_builder: Int64Builder,
    members_role_builder: StringBuilder,
    members_offsets: Vec<i32>,
    changeset_builder: Int64Builder,
    timestamp_builder: TimestampMillisecondBuilder,
    uid_builder: Int32Builder,
    user_builder: StringBuilder,
    version_builder: Int32Builder,
    visible_builder: BooleanBuilder,
}

impl Default for OSMArrowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OSMArrowBuilder {
    pub fn new() -> Self {
        OSMArrowBuilder {
            id_builder: Int64Builder::new(),
            tags_builder: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
            lat_builder: Float64Builder::new(),
            lon_builder: Float64Builder::new(),
            nodes_ref_builder: Int64Builder::new(),
            nodes_offsets: vec![0],
            members_type_builder: StringBuilder::new(),
            members_ref_builder: Int64Builder::new(),
            members_role_builder: StringBuilder::new(),
            members_offsets: vec![0],
            changeset_builder: Int64Builder::new(),
            timestamp_builder: TimestampMillisecondBuilder::new(),
            uid_builder: Int32Builder::new(),
            user_builder: StringBuilder::new(),
            version_builder: Int32Builder::new(),
            visible_builder: BooleanBuilder::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_row<'a, T, N, M>(
        &mut self,
        id: i64,
        _type_: OSMType,
        tags_iter: T,
        lat: Option<f64>,
        lon: Option<f64>,
        nodes_iter: N,
        members_iter: M,
        changeset: Option<i64>,
        timestamp_ms: Option<i64>,
        uid: Option<i32>,
        user: Option<&str>,
        version: Option<i32>,
        visible: Option<bool>,
    ) -> usize
    where
        T: IntoIterator<Item = (&'a str, &'a str)>,
        N: IntoIterator<Item = i64>,
        M: IntoIterator<Item = (OSMType, i64, Option<&'a str>)>,
    {
        // Track approximate size of inserted data, starting with known constant sizes
        let mut est_size_bytes = 64usize;

        self.id_builder.append_value(id);

        for (key, value) in tags_iter {
            est_size_bytes += key.len() + value.len();
            self.tags_builder.keys().append_value(key);
            self.tags_builder.values().append_value(value);
        }
        let _ = self.tags_builder.append(true);

        self.lat_builder.append_option(lat);
        self.lon_builder.append_option(lon);

        for node_id in nodes_iter {
            est_size_bytes += 8usize;
            self.nodes_ref_builder.append_value(node_id);
        }
        self.nodes_offsets.push(self.nodes_ref_builder.len() as i32);

        for (osm_type, ref_, role) in members_iter {
            // Rough size to avoid unwrapping, role should be fairly short.
            est_size_bytes += 10usize;
            self.members_type_builder.append_value(osm_type.as_str());
            self.members_ref_builder.append_value(ref_);
            self.members_role_builder.append_option(role);
        }
        self.members_offsets
            .push(self.members_ref_builder.len() as i32);

        self.changeset_builder.append_option(changeset);
        self.timestamp_builder.append_option(timestamp_ms);
        self.uid_builder.append_option(uid);
        self.user_builder.append_option(user);
        self.version_builder.append_option(version);
        self.visible_builder.append_option(visible);

        est_size_bytes
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let nodes_struct = StructArray::try_new(
            Fields::from(vec![Field::new("ref", DataType::Int64, true)]),
            vec![Arc::new(self.nodes_ref_builder.finish()) as ArrayRef],
            None,
        )?;
        let nodes_offsets = std::mem::replace(&mut self.nodes_offsets, vec![0]);
        let nodes_array = ListArray::try_new(
            Arc::new(Field::new("item", nodes_struct.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(nodes_offsets)),
            Arc::new(nodes_struct),
            None,
        )?;

        let members_struct = StructArray::try_new(
            Fields::from(vec![
                Field::new("type", DataType::Utf8, true),
                Field::new("ref", DataType::Int64, true),
                Field::new("role", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(self.members_type_builder.finish()) as ArrayRef,
                Arc::new(self.members_ref_builder.finish()) as ArrayRef,
                Arc::new(self.members_role_builder.finish()) as ArrayRef,
            ],
            None,
        )?;
        let members_offsets = std::mem::replace(&mut self.members_offsets, vec![0]);
        let members_array = ListArray::try_new(
            Arc::new(Field::new("item", members_struct.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(members_offsets)),
            Arc::new(members_struct),
            None,
        )?;

        let array_refs: Vec<ArrayRef> = vec![
            Arc::new(self.id_builder.finish()),
            Arc::new(self.tags_builder.finish()),
            Arc::new(self.lat_builder.finish()),
            Arc::new(self.lon_builder.finish()),
            Arc::new(nodes_array),
            Arc::new(members_array),
            Arc::new(self.changeset_builder.finish()),
            Arc::new(self.timestamp_builder.finish()),
            Arc::new(self.uid_builder.finish()),
            Arc::new(self.user_builder.finish()),
            Arc::new(self.version_builder.finish()),
            Arc::new(self.visible_builder.finish()),
        ];

        RecordBatch::try_new(cached_osm_schema(), array_refs)
    }
}
