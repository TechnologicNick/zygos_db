use std::io::{Cursor, Error, ErrorKind, Read};

#[inline]
pub fn read_u64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u64> {
    let mut tmp = [0; size_of::<u64>()];
    cursor.read_exact(&mut tmp)?;
    Ok(u64::from_be_bytes(tmp))
}

#[inline]
pub fn read_i64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<i64> {
    let mut tmp = [0; size_of::<i64>()];
    cursor.read_exact(&mut tmp)?;
    Ok(i64::from_be_bytes(tmp))
}

#[inline]
pub fn read_zigzag_i64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<(i64, usize)> {
    let mut tmp = [0u8; 9];
    cursor.read_exact(&mut tmp[0..1])?;
    let len = vint64::decoded_len(tmp[0]);

    cursor.read_exact(&mut tmp[1..len])?;
    let mut slice = &tmp[..len];

    let res = vint64::signed::decode(&mut slice)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!(
            "Failed to decode zigzag i64 (len={:?}, buf={:?}): {:?}",
            len, tmp, e,
        )))?;
    
    Ok((res, len))
}

#[inline]
pub fn read_f64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<f64> {
    let mut tmp = [0; size_of::<f64>()];
    cursor.read_exact(&mut tmp)?;
    Ok(f64::from_be_bytes(tmp))
}

#[inline]
pub fn read_u8(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u8> {
    let mut tmp = [0; size_of::<u8>()];
    cursor.read_exact(&mut tmp)?;
    Ok(tmp[0])
}

#[inline]
pub fn read_string_u8(cursor: &mut Cursor<&[u8]>) -> std::io::Result<String> {
    let len = read_u8(cursor)? as usize;
    let mut tmp = vec![0; len];
    cursor.read_exact(&mut tmp)?;
    Ok(String::from_utf8(tmp).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
}



#[inline]
pub fn skip_zigzag_i64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<usize> {
    let mut tmp = [0u8; 9];
    cursor.read_exact(&mut tmp[0..1])?;
    let len = vint64::decoded_len(tmp[0]);

    cursor.read_exact(&mut tmp[1..len])?;
    Ok(len)
}

#[inline]
pub fn skip_f64(cursor: &mut Cursor<&[u8]>) -> std::io::Result<usize> {
    let mut tmp = [0; size_of::<f64>()];
    cursor.read_exact(&mut tmp)?;
    Ok(size_of::<f64>())
}

#[inline]
pub fn skip_string_u8(cursor: &mut Cursor<&[u8]>) -> std::io::Result<usize> {
    let len = read_u8(cursor)? as usize;
    let mut tmp = [0; u8::MAX as usize];
    cursor.read_exact(&mut tmp[0..len])?;
    Ok(1 + len)
}
