use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use clap::Parser;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use url::Url;

use crate::ElementSink;
use crate::error::{OsmPbfParquetError, OsmPbfParquetResult};
use crate::osm_arrow::OSMType;

pub type SinkpoolStore = HashMap<OSMType, Arc<Mutex<Vec<ElementSink>>>>;

// Write once, safe read across threads
pub static ARGS: OnceLock<Args> = OnceLock::new();

// Element counter to track read progress
pub static ELEMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

static BYTES_IN_MB: usize = 1024 * 1024;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to input PBF
    /// S3 URIs and filesystem paths are supported
    /// Note that reading from S3 is *much* slower, consider copying locally first
    #[arg(short, long)]
    pub input: String,

    /// Path to output directory
    /// S3 URIs and filesystem paths are supported
    #[arg(short, long, default_value = "./parquet")]
    pub output: String,

    /// Zstd compression level, 1-22, 0 for no compression
    #[arg(long, default_value_t = 3)]
    pub compression: u8,

    /// Worker thread count, default CPU count
    #[arg(long)]
    pub worker_threads: Option<usize>,

    /// Advanced options:
    ///
    /// Input buffer size, default 8MB
    #[arg(long)]
    pub input_buffer_size_mb: Option<usize>,

    /// Override target record batch size, balance this with available memory
    /// default is total memory (MB) / CPU count / 8
    #[arg(long)]
    pub record_batch_target_mb: Option<usize>,

    /// Max feature count per row group
    #[arg(long)]
    pub max_row_group_count: Option<usize>,

    /// Override target parquet file size, default 500MB
    #[arg(long, default_value_t = 500usize)]
    pub file_target_mb: usize,
}

impl Args {
    pub fn new(input: String, output: String, compression: u8) -> Self {
        Args {
            input,
            output,
            compression,
            worker_threads: None,
            input_buffer_size_mb: None,
            record_batch_target_mb: None,
            max_row_group_count: None,
            file_target_mb: 500usize,
        }
    }

    pub fn validate(&self) -> OsmPbfParquetResult<()> {
        if !self.input.ends_with(".pbf") && !self.input.ends_with(".osm.pbf") {
            return Err(OsmPbfParquetError::InvalidArgument(
                "input must end with .pbf or .osm.pbf".to_string(),
            ));
        }
        Self::validate_scheme(&self.input)?;
        Self::validate_scheme(&self.output)?;
        if let Some(threads) = self.worker_threads
            && threads == 0
        {
            return Err(OsmPbfParquetError::InvalidArgument(
                "worker_threads must be greater than 0".to_string(),
            ));
        }
        if self.file_target_mb == 0 {
            return Err(OsmPbfParquetError::InvalidArgument(
                "file_target_mb must be greater than 0".to_string(),
            ));
        }
        if self.compression > 22 {
            return Err(OsmPbfParquetError::InvalidArgument(
                "compression must be between 0 and 22".to_string(),
            ));
        }
        if let Some(input_buffer_size_mb) = self.input_buffer_size_mb
            && input_buffer_size_mb == 0
        {
            return Err(OsmPbfParquetError::InvalidArgument(
                "input_buffer_size_mb must be greater than 0".to_string(),
            ));
        }
        if let Some(record_batch_target_mb) = self.record_batch_target_mb
            && record_batch_target_mb == 0
        {
            return Err(OsmPbfParquetError::InvalidArgument(
                "record_batch_target_mb must be greater than 0".to_string(),
            ));
        }
        if let Some(max_row_group_count) = self.max_row_group_count
            && max_row_group_count == 0
        {
            return Err(OsmPbfParquetError::InvalidArgument(
                "max_row_group_count must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_scheme(path: &str) -> OsmPbfParquetResult<()> {
        // Only treat the input as a URL if it clearly looks like one (contains "://").
        // This avoids misclassifying Windows-style paths like "C:\\file.pbf" as URLs.
        if path.contains("://") {
            match Url::parse(path) {
                Ok(url) => {
                    if url.scheme() != "s3" {
                        return Err(OsmPbfParquetError::InvalidArgument(format!(
                            "unsupported URL scheme '{}', only s3:// and local paths are supported",
                            url.scheme()
                        )));
                    }
                }
                Err(_) => {
                    return Err(OsmPbfParquetError::InvalidArgument(format!(
                        "invalid URL '{}', only s3:// and local paths are supported",
                        path
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn get_worker_threads(&self) -> usize {
        self.worker_threads.unwrap_or_else(|| {
            let system = System::new_with_specifics(
                RefreshKind::nothing().with_cpu(CpuRefreshKind::nothing()),
            );
            system.cpus().len()
        })
    }

    pub fn get_input_buffer_size_bytes(&self) -> usize {
        // Max size of an uncompressed single blob is 32MB, assumes compression ratio of 2:1 or better
        self.input_buffer_size_mb.unwrap_or(16) * BYTES_IN_MB
    }

    pub fn get_record_batch_target_bytes(&self) -> usize {
        self.record_batch_target_mb
            .unwrap_or_else(default_record_batch_size_mb)
            * BYTES_IN_MB
    }

    pub fn get_file_target_bytes(&self) -> usize {
        self.file_target_mb * BYTES_IN_MB
    }
}

fn default_record_batch_size_mb() -> usize {
    let system = System::new_with_specifics(
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::nothing())
            .with_memory(MemoryRefreshKind::everything()),
    );
    let total_memory_mb = system.total_memory() as usize / BYTES_IN_MB;
    let cpu_count = system.cpus().len();
    // Estimate per thread available memory, leaving overhead for copies and system processes
    (total_memory_mb / cpu_count) / 8usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_args() -> Args {
        Args::new("test.osm.pbf".to_string(), "./out".to_string(), 3)
    }

    #[test]
    fn test_validate_valid_pbf() {
        let args = Args::new("file.pbf".to_string(), "./out".to_string(), 3);
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_valid_osm_pbf() {
        assert!(valid_args().validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_input_extension() {
        let args = Args::new("file.xml".to_string(), "./out".to_string(), 3);
        let err = args.validate().unwrap_err();
        assert!(matches!(err, OsmPbfParquetError::InvalidArgument(_)));
    }

    #[test]
    fn test_validate_worker_threads_zero() {
        let mut args = valid_args();
        args.worker_threads = Some(0);
        let err = args.validate().unwrap_err();
        assert!(matches!(err, OsmPbfParquetError::InvalidArgument(_)));
    }

    #[test]
    fn test_validate_worker_threads_valid() {
        let mut args = valid_args();
        args.worker_threads = Some(4);
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_worker_threads_none() {
        let args = valid_args();
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_file_target_mb_zero() {
        let mut args = valid_args();
        args.file_target_mb = 0;
        let err = args.validate().unwrap_err();
        assert!(matches!(err, OsmPbfParquetError::InvalidArgument(_)));
    }
}
