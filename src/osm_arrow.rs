use std::fmt;
use std::sync::Arc;

use arrow::array::builder::{
    BooleanBuilder, Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
};
use arrow::array::{ArrayRef, Float64Builder, Int32Builder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum OSMType {
    Node,
    Way,
    Relation,
}

impl fmt::Display for OSMType {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{}", format!("{:?}", self).to_lowercase())
    }
}

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

pub struct OSMArrowBuilder {
    id_builder: Box<Int64Builder>,
    tags_builder: Box<MapBuilder<StringBuilder, StringBuilder>>,
    lat_builder: Box<Float64Builder>,
    lon_builder: Box<Float64Builder>,
    nodes_builder: Box<ListBuilder<StructBuilder>>,
    members_builder: Box<ListBuilder<StructBuilder>>,
    changeset_builder: Box<Int64Builder>,
    timestamp_builder: Box<TimestampMillisecondBuilder>,
    uid_builder: Box<Int32Builder>,
    user_builder: Box<StringBuilder>,
    version_builder: Box<Int32Builder>,
    visible_builder: Box<BooleanBuilder>,
}

impl Default for OSMArrowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OSMArrowBuilder {
    pub fn new() -> Self {
        let id_builder = Box::new(Int64Builder::new());
        let tags_builder = Box::new(MapBuilder::new(
            None,
            StringBuilder::new(),
            StringBuilder::new(),
        ));
        let lat_builder = Box::new(Float64Builder::new());
        let lon_builder = Box::new(Float64Builder::new());
        let nodes_builder = Box::new(ListBuilder::new(StructBuilder::from_fields(
            vec![Field::new("ref", DataType::Int64, true)],
            0,
        )));
        let members_builder = Box::new(ListBuilder::new(StructBuilder::from_fields(
            vec![
                Field::new("type", DataType::Utf8, true),
                Field::new("ref", DataType::Int64, true),
                Field::new("role", DataType::Utf8, true),
            ],
            0,
        )));
        let changeset_builder = Box::new(Int64Builder::new());
        let timestamp_builder = Box::new(TimestampMillisecondBuilder::new());
        let uid_builder = Box::new(Int32Builder::new());
        let user_builder = Box::new(StringBuilder::new());
        let version_builder = Box::new(Int32Builder::new());
        let visible_builder = Box::new(BooleanBuilder::new());

        OSMArrowBuilder {
            id_builder,
            tags_builder,
            lat_builder,
            lon_builder,
            nodes_builder,
            members_builder,
            changeset_builder,
            timestamp_builder,
            uid_builder,
            user_builder,
            version_builder,
            visible_builder,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_row<T, N, M>(
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
        user: Option<String>,
        version: Option<i32>,
        visible: Option<bool>,
    ) -> usize
    where
        T: IntoIterator<Item = (String, String)>,
        N: IntoIterator<Item = i64>,
        M: IntoIterator<Item = (OSMType, i64, Option<String>)>,
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

        // Derived from https://docs.rs/arrow/latest/arrow/array/struct.StructBuilder.html
        let struct_builder = self.nodes_builder.values();
        for node_id in nodes_iter {
            est_size_bytes += 8usize;
            struct_builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(node_id);
            struct_builder.append(true);
        }
        self.nodes_builder.append(true);

        let members_struct_builder = self.members_builder.values();
        for (osm_type, ref_, role) in members_iter {
            // Rough size to avoid unwrapping, role should be fairly short.
            est_size_bytes += 10usize;

            members_struct_builder
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(osm_type.to_string());

            members_struct_builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(ref_);

            members_struct_builder
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_option(role);

            members_struct_builder.append(true);
        }
        self.members_builder.append(true);

        self.changeset_builder.append_option(changeset);
        self.timestamp_builder.append_option(timestamp_ms);
        self.uid_builder.append_option(uid);
        self.user_builder.append_option(user);
        self.version_builder.append_option(version);
        self.visible_builder.append_option(visible);

        est_size_bytes
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let array_refs: Vec<ArrayRef> = vec![
            Arc::new(self.id_builder.finish()),
            Arc::new(self.tags_builder.finish()),
            Arc::new(self.lat_builder.finish()),
            Arc::new(self.lon_builder.finish()),
            Arc::new(self.nodes_builder.finish()),
            Arc::new(self.members_builder.finish()),
            Arc::new(self.changeset_builder.finish()),
            Arc::new(self.timestamp_builder.finish()),
            Arc::new(self.uid_builder.finish()),
            Arc::new(self.user_builder.finish()),
            Arc::new(self.version_builder.finish()),
            Arc::new(self.visible_builder.finish()),
        ];

        RecordBatch::try_new(Arc::new(osm_arrow_schema()), array_refs)
    }
}
