//! Hand-rolled standard (RFC 4648) base64, replacing the `base64` crate -
//! the only encoding this crate ever needed was its `STANDARD` engine's
//! `encode`/`decode` (see `encryption::encrypt_leaf`/`decrypt_leaf` and
//! `shared::hash_secret`), so a ~70-line implementation is cheaper than
//! the dependency.
//!
//! Uses the standard alphabet (`A-Za-z0-9+/`) and `=` padding, matching
//! `base64::engine::general_purpose::STANDARD` byte for byte. [`decode`]
//! is strict the same way `STANDARD` is: the input's length must be a
//! multiple of four, padding may only appear as the last one or two
//! characters of the final group, and a non-alphabet byte anywhere else is
//! rejected - which is what lets `decrypt_leaf` treat a tampered ciphertext
//! string as a decryption failure rather than silently decoding it.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encodes `input` as standard base64 with `=` padding.
pub fn encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Decodes standard base64 with `=` padding. `None` on any of the
/// malformations [`decode`]'s own doc comment lists.
pub fn decode(input: &str) -> Option<Vec<u8>> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut saw_padding = false;
    for group in bytes.chunks(4) {
        // Padding ends the input: no group may follow one that carried it.
        if saw_padding {
            return None;
        }
        let mut sextets = [0u8; 4];
        let mut padding = 0usize;
        for (i, &byte) in group.iter().enumerate() {
            if byte == b'=' {
                // Padding is only meaningful as the last one or two
                // characters; a `=` at positions 0/1 is never valid.
                if i < 2 {
                    return None;
                }
                padding += 1;
            } else {
                // Data after padding has already started is invalid too.
                if padding > 0 {
                    return None;
                }
                sextets[i] = decode_sextet(byte)?;
            }
        }
        saw_padding = padding > 0;
        let triple = ((sextets[0] as u32) << 18)
            | ((sextets[1] as u32) << 12)
            | ((sextets[2] as u32) << 6)
            | (sextets[3] as u32);
        out.push((triple >> 16) as u8);
        if padding < 2 {
            out.push((triple >> 8) as u8);
        }
        if padding < 1 {
            out.push(triple as u8);
        }
    }
    Some(out)
}

fn decode_sextet(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn round_trips_every_length_modulo_three() {
        for len in 0..64usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + 1) as u8).collect();
            let encoded = encode(&bytes);
            assert_eq!(decode(&encoded).as_deref(), Some(bytes.as_slice()));
        }
    }

    #[test]
    fn matches_known_vectors() {
        // RFC 4648 §10's own test vectors.
        assert_eq!(encode(b""), "");
        assert_eq!(encode(b"f"), "Zg==");
        assert_eq!(encode(b"fo"), "Zm8=");
        assert_eq!(encode(b"foo"), "Zm9v");
        assert_eq!(encode(b"foob"), "Zm9vYg==");
        assert_eq!(encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(decode("Zm9vYmFy").as_deref(), Some(b"foobar".as_slice()));
    }

    #[test]
    fn rejects_malformed_input() {
        // Not a multiple of four.
        assert!(decode("Zg=").is_none());
        // Padding in a position it can't occupy.
        assert!(decode("=AAA").is_none());
        // Data after padding.
        assert!(decode("Zg==Zg==").is_none());
        // A byte outside the alphabet.
        assert!(decode("Zg!!").is_none());
    }
}
