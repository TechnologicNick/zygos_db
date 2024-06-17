#![allow(dead_code)]
#![feature(btree_cursors)]

mod tsv_reader;
mod config;
mod database;

pub mod query;
pub use tsv_reader::ColumnType;
pub mod compression;
pub mod deserialize;
