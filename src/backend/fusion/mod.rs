use std::ops::Deref;

use anyhow::Result;
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::{
    dataframe::DataFrame,
    execution::{
        config::SessionConfig,
        context::SessionContext,
        options::{CsvReadOptions, NdJsonReadOptions},
    },
};
use describe::DataFrameDescriber;

use crate::{Backend, ConnectOpts, DatasetConn, ReplDisplay};

mod describe;
mod df_describe;

pub struct DataFusionBackend(SessionContext);

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        let ctx = SessionContext::new_with_config(config);
        Self(ctx)
    }
}

impl Backend for DataFusionBackend {
    async fn connect(&mut self, opts: &ConnectOpts) -> Result<()> {
        match &opts.conn {
            DatasetConn::Postgres(_conn_str) => {
                println!("Postgres connection is not supported yet")
            }
            DatasetConn::Csv(file_opts) => {
                let csv_opts = CsvReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };

                self.register_csv(&opts.name, &file_opts.filename, csv_opts)
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::MdJson(file_opts) => {
                let json_opts = NdJsonReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };

                self.register_json(&opts.name, &file_opts.filename, json_opts)
                    .await?;
            }
        }
        Ok(())
    }

    async fn list(&mut self) -> Result<impl crate::ReplDisplay> {
        let sql = "select table_name, table_type from information_schema.tables where table_schema = 'public'";
        let df = self.0.sql(sql).await?;
        Ok(df)
    }

    async fn schema(&self, name: &str) -> Result<impl crate::ReplDisplay> {
        let df = self.0.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }

    async fn describe(&mut self, name: &str) -> Result<impl crate::ReplDisplay> {
        let df = self.0.sql(&format!("select * from {}", name)).await?;
        let ddf = DataFrameDescriber::try_new(df)?;
        ddf.describe().await
    }

    async fn head(&mut self, name: &str, size: usize) -> Result<impl crate::ReplDisplay> {
        let df = self
            .0
            .sql(&format!("SELECT * FROM {} LIMIT {}", name, size))
            .await?;
        Ok(df)
    }

    async fn sql(&mut self, sql: &str) -> Result<impl crate::ReplDisplay> {
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for DataFrame {
    async fn display(self) -> Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}

impl ReplDisplay for RecordBatch {
    async fn display(self) -> Result<String> {
        let data = pretty_format_batches(&[self])?;
        Ok(data.to_string())
    }
}
