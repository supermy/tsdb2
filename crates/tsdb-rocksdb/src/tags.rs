use crate::error::Result;
use crate::error::TsdbRocksDbError;
use tsdb_arrow::schema::Tags;

/// 将 u16 编码为大端序并追加到缓冲区
fn encode_u16(buf: &mut Vec<u8>, val: u16) {
    buf.extend_from_slice(&val.to_be_bytes());
}

/// 从字节数组的大端序中解码 u16，推进游标
fn decode_u16(data: &[u8], cursor: &mut usize) -> Result<u16> {
    if *cursor + 2 > data.len() {
        return Err(TsdbRocksDbError::InvalidValue(
            "truncated u16 in tags".to_string(),
        ));
    }
    let val = u16::from_be_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    Ok(val)
}

/// 编码字符串: [长度: u16 BE][utf8 字节]
fn encode_str(buf: &mut Vec<u8>, s: &str) {
    encode_u16(buf, s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
}

/// 解码字符串，推进游标
fn decode_str(data: &[u8], cursor: &mut usize) -> Result<String> {
    let len = decode_u16(data, cursor)? as usize;
    if *cursor + len > data.len() {
        return Err(TsdbRocksDbError::InvalidValue(
            "truncated string in tags".to_string(),
        ));
    }
    let s = std::string::String::from_utf8(data[*cursor..*cursor + len].to_vec())
        .map_err(crate::error::TsdbRocksDbError::FromUtf8)?;
    *cursor += len;
    Ok(s)
}

/// 将标签集合编码为紧凑二进制格式
///
/// 编码格式 (用于 `_series_meta` CF 的 Value):
/// ```text
/// [num_tags: u16]
/// [tag_1: [key_len: u16][key_bytes][val_len: u16][val_bytes]]
/// [tag_2: ...]
/// ...
/// ```
pub fn encode_tags(tags: &Tags) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    encode_u16(&mut buf, tags.len() as u16);
    for (k, v) in tags.iter() {
        encode_str(&mut buf, k);
        encode_str(&mut buf, v);
    }
    buf
}

/// 从紧凑二进制格式解码标签集合
pub fn decode_tags(data: &[u8]) -> Result<Tags> {
    let mut cursor = 0;
    let num_tags = decode_u16(data, &mut cursor)?;
    let mut tags = Tags::new();
    for _ in 0..num_tags {
        let k = decode_str(data, &mut cursor)?;
        let v = decode_str(data, &mut cursor)?;
        tags.insert(k, v);
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tags_encode_decode_roundtrip() {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), "server01".to_string());
        tags.insert("region".to_string(), "us-west".to_string());

        let encoded = encode_tags(&tags);
        let decoded = decode_tags(&encoded).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.get("host").unwrap(), "server01");
        assert_eq!(decoded.get("region").unwrap(), "us-west");
    }

    #[test]
    fn test_tags_empty() {
        let tags = Tags::new();
        let encoded = encode_tags(&tags);
        let decoded = decode_tags(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_tags_single_tag() {
        let mut tags = Tags::new();
        tags.insert("key".to_string(), "value".to_string());

        let encoded = encode_tags(&tags);
        let decoded = decode_tags(&encoded).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.get("key").unwrap(), "value");
    }

    #[test]
    fn test_tags_unicode() {
        let mut tags = Tags::new();
        tags.insert("主机".to_string(), "服务器-01".to_string());

        let encoded = encode_tags(&tags);
        let decoded = decode_tags(&encoded).unwrap();

        assert_eq!(decoded.get("主机").unwrap(), "服务器-01");
    }

    #[test]
    fn test_tags_many_tags() {
        let mut tags = Tags::new();
        for i in 0..50 {
            tags.insert(format!("tag_{}", i), format!("val_{}", i));
        }

        let encoded = encode_tags(&tags);
        let decoded = decode_tags(&encoded).unwrap();

        assert_eq!(decoded.len(), 50);
        for i in 0..50 {
            assert_eq!(
                decoded.get(&format!("tag_{}", i)).unwrap(),
                &format!("val_{}", i)
            );
        }
    }

    #[test]
    fn test_tags_truncated_error() {
        let result = decode_tags(&[0x00, 0x01]);
        assert!(result.is_err());
    }
}
