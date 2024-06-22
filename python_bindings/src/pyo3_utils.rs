// Copyright (c) 2023-present PyO3 Project and Contributors.  https://github.com/PyO3
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

// These functions are copied from PyO3, as list::new_from_iter is not public.

use pyo3::{ffi::{self, Py_ssize_t}, types::{PyAnyMethods, PyList}, Bound, PyAny, PyObject, Python};

#[inline]
#[track_caller]
pub(crate) fn new_from_iter<'py>(
    py: Python<'py>,
    len: usize,
    elements: &mut dyn Iterator<Item = PyObject>,
) -> Bound<'py, PyList> {
    unsafe {
        // PyList_New checks for overflow but has a bad error message, so we check ourselves
        let len: Py_ssize_t = len
            .try_into()
            .expect("out of range integral type conversion attempted on `elements.len()`");

        let ptr = ffi::PyList_New(len);

        // We create the `Bound` pointer here for two reasons:
        // - panics if the ptr is null
        // - its Drop cleans up the list if user code or the asserts panic.
        let list = ptr.assume_owned(py).downcast_into_unchecked();

        let mut counter: Py_ssize_t = 0;

        for obj in elements.take(len as usize) {
            // #[cfg(not(Py_LIMITED_API))]
            ffi::PyList_SET_ITEM(ptr, counter, obj.into_ptr());
            // #[cfg(Py_LIMITED_API)]
            // ffi::PyList_SetItem(ptr, counter, obj.into_ptr());
            counter += 1;
        }

        assert!(elements.next().is_none(), "Attempted to create PyList but `elements` was larger than reported by its `ExactSizeIterator` implementation.");
        assert_eq!(len, counter, "Attempted to create PyList but `elements` was smaller than reported by its `ExactSizeIterator` implementation.");

        list
    }
}

pub(crate) trait FfiPtrExt: Sealed {
    unsafe fn assume_owned(self, py: Python<'_>) -> Bound<'_, PyAny>;
}

impl FfiPtrExt for *mut ffi::PyObject {
    #[inline]
    unsafe fn assume_owned(self, py: Python<'_>) -> Bound<'_, PyAny> {
        Bound::from_owned_ptr(py, self)
    }
}

pub trait Sealed {}

// for FfiPtrExt
impl Sealed for *mut ffi::PyObject {}
