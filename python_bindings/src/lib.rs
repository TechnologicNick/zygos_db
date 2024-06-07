use std::{collections::BTreeMap, fs::OpenOptions};

use pyo3::prelude::*;

#[pyclass]
struct DatabaseQueryClient {
    inner: zygos_db::query::DatabaseQueryClient<std::fs::File>,
}

#[pymethods]
impl DatabaseQueryClient {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
        Ok(Self {
            inner: zygos_db::query::DatabaseQueryClient::new(file),
        })
    }

    fn read_table_index(&mut self, offset: u64) -> PyResult<TableIndex> {
        let index = self.inner.read_table_index(offset)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
        Ok(TableIndex { inner: index })
    }
}

#[pyclass]
struct TableIndex {
    inner: BTreeMap<u64, u64>,
}

#[pymethods]
impl TableIndex {
    fn get_all(&self) -> PyResult<Vec<(u64, u64)>> {
        Ok(self.inner.iter().map(|(k, v)| (*k, *v)).collect())
    }
}

/// A Python module to read ZygosDB files.
#[pymodule]
#[pyo3(name = "zygos_db")]
fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatabaseQueryClient>()?;
    Ok(())
}
