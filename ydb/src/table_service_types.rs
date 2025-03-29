use crate::grpc_wrapper::raw_table_service::copy_table::RawCopyTableItem;

#[derive(Clone)]
pub struct CopyTableItem {
    inner: RawCopyTableItem,
}

impl CopyTableItem {
    #[allow(dead_code)]
    pub fn new(source_path: String, destination_path: String, omit_indexes: bool) -> Self {
        Self {
            inner: RawCopyTableItem {
                source_path,
                destination_path,
                omit_indexes,
            },
        }
    }
}

impl From<CopyTableItem> for RawCopyTableItem {
    fn from(value: CopyTableItem) -> Self {
        value.inner
    }
}
