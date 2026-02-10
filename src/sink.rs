use object_store::buffered::BufWriter;
use std::path::absolute;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use osmpbf::{DenseNode, Node, RelMemberType, Relation, Way};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::osm_arrow::{OSMArrowBuilder, OSMType, cached_osm_schema};
use crate::util::ARGS;

pub struct ElementSink {
    // Config for writing file
    pub osm_type: OSMType,
    filenum: Arc<Mutex<u64>>,

    // Arrow wrappers
    osm_builder: Box<OSMArrowBuilder>,
    writer: Option<AsyncArrowWriter<BufWriter>>, // Wrapped so we can replace this on the fly

    // State tracking for batching
    estimated_record_batch_bytes: usize,
    estimated_file_bytes: usize,
    target_record_batch_bytes: usize,
    target_file_bytes: usize,
    pub last_write_cycle: Instant,
}

impl ElementSink {
    pub fn new(filenum: Arc<Mutex<u64>>, osm_type: OSMType) -> Result<Self, anyhow::Error> {
        let args = ARGS.get().expect("ARGS not initialized");

        let full_path = Self::create_full_path(&args.output, &osm_type, &filenum, args.compression);
        let buf_writer = Self::create_buf_writer(&full_path)?;
        let writer = Self::create_writer(buf_writer, args.compression, args.max_row_group_count)?;

        Ok(ElementSink {
            osm_type,
            filenum,

            osm_builder: Box::new(OSMArrowBuilder::new()),
            writer: Some(writer),

            estimated_record_batch_bytes: 0usize,
            estimated_file_bytes: 0usize,
            target_record_batch_bytes: args.get_record_batch_target_bytes(),
            target_file_bytes: args.get_file_target_bytes(),
            last_write_cycle: Instant::now(),
        })
    }

    pub async fn finish(&mut self) -> Result<(), anyhow::Error> {
        self.finish_batch().await?;
        self.writer.take().unwrap().close().await?;
        Ok(())
    }

    async fn finish_batch(&mut self) -> Result<(), anyhow::Error> {
        if self.estimated_record_batch_bytes == 0 {
            // Nothing to write
            return Ok(());
        }
        let batch = self.osm_builder.finish()?;
        self.writer.as_mut().unwrap().write(&batch).await?;

        // Reset writer to new path if needed
        self.estimated_file_bytes += self.estimated_record_batch_bytes;
        if self.estimated_file_bytes >= self.target_file_bytes {
            self.writer.take().unwrap().close().await?;

            // Create new writer and output
            let args = ARGS.get().expect("ARGS not initialized");
            let full_path = Self::create_full_path(
                &args.output,
                &self.osm_type,
                &self.filenum,
                args.compression,
            );
            let buf_writer = Self::create_buf_writer(&full_path)?;
            self.writer = Some(Self::create_writer(
                buf_writer,
                args.compression,
                args.max_row_group_count,
            )?);
            self.estimated_file_bytes = 0;
        }

        self.estimated_record_batch_bytes = 0;
        Ok(())
    }

    pub async fn increment_and_cycle(&mut self) -> Result<(), anyhow::Error> {
        self.last_write_cycle = Instant::now();
        if self.estimated_record_batch_bytes >= self.target_record_batch_bytes {
            self.finish_batch().await?;
        }
        Ok(())
    }

    fn create_buf_writer(full_path: &str) -> Result<BufWriter, anyhow::Error> {
        // TODO - better validation of URL/paths here and error handling
        if let Ok(url) = Url::parse(full_path) {
            let s3_store = AmazonS3Builder::from_env().with_url(url.clone()).build()?;
            let path = Path::parse(url.path())?;

            Ok(BufWriter::new(Arc::new(s3_store), path))
        } else {
            let object_store = LocalFileSystem::new();
            let absolute_path = absolute(full_path)?;
            let store_path = Path::from_absolute_path(absolute_path)?;

            Ok(BufWriter::new(Arc::new(object_store), store_path))
        }
    }

    fn create_writer(
        buffer: BufWriter,
        compression: u8,
        max_row_group_rows: Option<usize>,
    ) -> Result<AsyncArrowWriter<BufWriter>, anyhow::Error> {
        let mut props_builder = WriterProperties::builder();
        if compression == 0 {
            props_builder = props_builder.set_compression(Compression::UNCOMPRESSED);
        } else if compression > 0 && compression <= 22 {
            props_builder = props_builder
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(compression as i32)?));
        }
        if let Some(max_rows) = max_row_group_rows {
            props_builder = props_builder.set_max_row_group_size(max_rows);
        }
        let props = props_builder.build();

        let writer = AsyncArrowWriter::try_new(buffer, cached_osm_schema(), Some(props))?;
        Ok(writer)
    }

    fn create_full_path(
        output_path: &str,
        osm_type: &OSMType,
        filenum: &Arc<Mutex<u64>>,
        compression: u8,
    ) -> String {
        let trailing_path = Self::new_trailing_path(osm_type, filenum, compression != 0);
        // Remove trailing `/`s to avoid empty path segment
        format!("{0}{trailing_path}", &output_path.trim_end_matches('/'))
    }

    fn new_trailing_path(
        osm_type: &OSMType,
        filenum: &Arc<Mutex<u64>>,
        is_zstd_compression: bool,
    ) -> String {
        let mut num = filenum.lock().unwrap();
        let compression_stem = if is_zstd_compression { ".zstd" } else { "" };
        let path = format!(
            "/type={}/{}_{:04}{}.parquet",
            osm_type, osm_type, num, compression_stem
        );
        *num += 1;
        path
    }

    pub fn add_node(&mut self, node: &Node) {
        let info = node.info();
        let user = info.user().unwrap_or(Ok("")).unwrap_or("");

        let est_size_bytes = self.osm_builder.append_row(
            node.id(),
            OSMType::Node,
            node.tags(),
            Some(node.lat()),
            Some(node.lon()),
            std::iter::empty(),
            std::iter::empty(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );
        self.estimated_record_batch_bytes += est_size_bytes;
    }

    pub fn add_dense_node(&mut self, node: &DenseNode) {
        let info = node.info();
        let user = info.map(|info| info.user().unwrap_or(""));

        let est_size_bytes = self.osm_builder.append_row(
            node.id(),
            OSMType::Node,
            node.tags(),
            Some(node.lat()),
            Some(node.lon()),
            std::iter::empty(),
            std::iter::empty(),
            info.map(|info| info.changeset()),
            info.map(|info| info.milli_timestamp()),
            info.map(|info| info.uid()),
            user,
            info.map(|info| info.version()),
            info.map(|info| info.visible()),
        );
        self.estimated_record_batch_bytes += est_size_bytes;
    }

    pub fn add_way(&mut self, way: &Way) {
        let info = way.info();
        let user = info.user().unwrap_or(Ok("")).unwrap_or("");

        let est_size_bytes = self.osm_builder.append_row(
            way.id(),
            OSMType::Way,
            way.tags(),
            None,
            None,
            way.refs(),
            std::iter::empty(),
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );
        self.estimated_record_batch_bytes += est_size_bytes;
    }

    pub fn add_relation(&mut self, relation: &Relation) {
        let info = relation.info();
        let user = info.user().unwrap_or(Ok("")).unwrap_or("");

        let members_iter = relation.members().map(|member| {
            let type_ = match member.member_type {
                RelMemberType::Node => OSMType::Node,
                RelMemberType::Way => OSMType::Way,
                RelMemberType::Relation => OSMType::Relation,
            };

            let role = member.role().ok();
            (type_, member.member_id, role)
        });

        let est_size_bytes = self.osm_builder.append_row(
            relation.id(),
            OSMType::Relation,
            relation.tags(),
            None,
            None,
            std::iter::empty(),
            members_iter,
            info.changeset(),
            info.milli_timestamp(),
            info.uid(),
            Some(user),
            info.version(),
            Some(info.visible()),
        );
        self.estimated_record_batch_bytes += est_size_bytes;
    }
}
