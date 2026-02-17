pub type OsmPbfParquetResult<T> = Result<T, OsmPbfParquetError>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OsmPbfParquetError {
    // External errors (foreign errors)
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    ObjectStorePath(#[from] object_store::path::Error),
    #[error(transparent)]
    TokioJoin(#[from] tokio::task::JoinError),
    #[error(transparent)]
    SemaphoreAcquire(#[from] tokio::sync::AcquireError),

    // Domain errors
    #[error("unknown blob type: {0}")]
    UnknownBlobType(String),
    #[error("blob decode error: {0}")]
    BlobDecode(String),
    #[error("writer already closed")]
    WriterClosed,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
