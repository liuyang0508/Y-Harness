//! Allocation-time bounds for JSON values and serialization.

use std::io::{self, Write};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(crate) const MAX_JSON_DEPTH: usize = 64;
pub(crate) const MAX_JSON_NODES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundedJsonError {
    LimitExceeded,
    CannotEncode,
}

pub(crate) fn validate_value_shape(value: &Value) -> Result<(), BoundedJsonError> {
    let mut pending = vec![(value, 0_usize)];
    let mut visited = 0_usize;
    while let Some((value, depth)) = pending.pop() {
        visited = visited
            .checked_add(1)
            .ok_or(BoundedJsonError::LimitExceeded)?;
        if depth > MAX_JSON_DEPTH || visited > MAX_JSON_NODES {
            return Err(BoundedJsonError::LimitExceeded);
        }
        let child_depth = depth.saturating_add(1);
        match value {
            Value::Array(values) => {
                validate_children(visited, pending.len(), depth, values.len())?;
                pending.extend(values.iter().map(|child| (child, child_depth)));
            }
            Value::Object(values) => {
                validate_children(visited, pending.len(), depth, values.len())?;
                pending.extend(values.values().map(|child| (child, child_depth)));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_children(
    visited: usize,
    pending: usize,
    depth: usize,
    children: usize,
) -> Result<(), BoundedJsonError> {
    if (children > 0 && depth >= MAX_JSON_DEPTH)
        || children
            > MAX_JSON_NODES
                .saturating_sub(visited)
                .saturating_sub(pending)
    {
        Err(BoundedJsonError::LimitExceeded)
    } else {
        Ok(())
    }
}

pub(crate) fn bounded_serialized_size<T: Serialize>(
    value: &T,
    maximum: usize,
) -> Result<usize, BoundedJsonError> {
    let mut writer = CountingWriter {
        bytes: 0,
        maximum,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.exceeded => Err(BoundedJsonError::LimitExceeded),
        Err(_) => Err(BoundedJsonError::CannotEncode),
    }
}

pub(crate) fn to_bounded_json_vec<T: Serialize>(
    value: &T,
    maximum: usize,
) -> Result<Vec<u8>, BoundedJsonError> {
    let mut writer = BoundedVecWriter {
        bytes: Vec::with_capacity(maximum.min(65_536)),
        maximum,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.exceeded => Err(BoundedJsonError::LimitExceeded),
        Err(_) => Err(BoundedJsonError::CannotEncode),
    }
}

pub(crate) fn bounded_serialized_sha256<T: Serialize>(
    value: &T,
    maximum: usize,
) -> Result<String, BoundedJsonError> {
    let mut writer = DigestWriter {
        hasher: Sha256::new(),
        bytes: 0,
        maximum,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => {
            let digest = writer.hasher.finalize();
            let mut encoded = String::with_capacity(64);
            const HEX: &[u8; 16] = b"0123456789abcdef";
            for byte in digest {
                encoded.push(char::from(HEX[usize::from(byte >> 4)]));
                encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
            Ok(encoded)
        }
        Err(_) if writer.exceeded => Err(BoundedJsonError::LimitExceeded),
        Err(_) => Err(BoundedJsonError::CannotEncode),
    }
}

struct CountingWriter {
    bytes: usize,
    maximum: usize,
    exceeded: bool,
}

struct DigestWriter {
    hasher: Sha256,
    bytes: usize,
    maximum: usize,
    exceeded: bool,
}

impl Write for DigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(next) = self.bytes.checked_add(buffer.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("JSON byte count overflow"));
        };
        if next > self.maximum {
            self.exceeded = true;
            return Err(io::Error::other("JSON exceeds byte limit"));
        }
        self.hasher.update(buffer);
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(next) = self.bytes.checked_add(buffer.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("JSON byte count overflow"));
        };
        if next > self.maximum {
            self.exceeded = true;
            return Err(io::Error::other("JSON exceeds byte limit"));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl Write for BoundedVecWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|length| length > self.maximum)
        {
            self.exceeded = true;
            return Err(io::Error::other("JSON exceeds byte limit"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sha2::Digest;

    use super::{
        BoundedJsonError, MAX_JSON_DEPTH, MAX_JSON_NODES, bounded_serialized_sha256,
        bounded_serialized_size, to_bounded_json_vec, validate_value_shape,
    };

    #[test]
    fn bounded_serialization_accepts_exact_limit_and_rejects_one_more_byte() {
        let value = json!({"value": "bounded"});
        let encoded = serde_json::to_vec(&value).expect("fixture");
        assert_eq!(
            bounded_serialized_size(&value, encoded.len()).expect("exact size"),
            encoded.len()
        );
        assert_eq!(
            to_bounded_json_vec(&value, encoded.len()).expect("exact encoding"),
            encoded
        );
        assert_eq!(
            to_bounded_json_vec(&value, encoded.len() - 1),
            Err(BoundedJsonError::LimitExceeded)
        );
        let expected_sha256 = sha2::Sha256::digest(&encoded)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            bounded_serialized_sha256(&value, encoded.len()).expect("digest"),
            expected_sha256
        );
        assert_eq!(
            bounded_serialized_sha256(&value, encoded.len() - 1),
            Err(BoundedJsonError::LimitExceeded)
        );
    }

    #[test]
    fn value_shape_is_bounded_before_pending_growth() {
        let too_wide = serde_json::Value::Array(vec![serde_json::Value::Null; MAX_JSON_NODES]);
        assert_eq!(
            validate_value_shape(&too_wide),
            Err(BoundedJsonError::LimitExceeded)
        );

        let mut too_deep = serde_json::Value::Null;
        for _ in 0..=MAX_JSON_DEPTH {
            too_deep = serde_json::Value::Array(vec![too_deep]);
        }
        assert_eq!(
            validate_value_shape(&too_deep),
            Err(BoundedJsonError::LimitExceeded)
        );
    }
}
