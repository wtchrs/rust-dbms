use std::fmt::{Debug, Formatter};

pub fn encode(elems: impl Iterator<Item = impl AsRef<[u8]>>, bytes: &mut Vec<u8>) {
    elems.for_each(|elem| {
        let elem_bytes = elem.as_ref();
        let len = internal::encoded_size(elem_bytes.len());
        bytes.reserve(len);
        internal::encode(elem_bytes, bytes);
    });
}

pub fn decode(bytes: &[u8], elems: &mut Vec<Vec<u8>>) {
    let mut rest = bytes;
    while !rest.is_empty() {
        let mut elem = vec![];
        internal::decode(&mut rest, &mut elem);
        elems.push(elem);
    }
}

pub struct Pretty<'a, T>(pub &'a [T]);

impl<'a, T: AsRef<[u8]>> Debug for Pretty<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_tuple("Tuple");
        for elem in self.0 {
            let bytes = elem.as_ref();
            match std::str::from_utf8(bytes) {
                Ok(s) => {
                    d.field(&format_args!("{:?} {:02x?}", s, bytes));
                }
                Err(_) => {
                    d.field(&format_args!("{:02x?}", bytes));
                }
            }
        }
        d.finish()
    }
}

mod internal {
    use std::cmp;

    pub const ESCAPE_LENGTH: usize = 9;

    pub fn encoded_size(len: usize) -> usize {
        (len + (ESCAPE_LENGTH - 1)) / (ESCAPE_LENGTH - 1) * ESCAPE_LENGTH
    }

    pub fn encode(mut src: &[u8], dst: &mut Vec<u8>) {
        loop {
            let copy_len = cmp::min(ESCAPE_LENGTH - 1, src.len());
            dst.extend_from_slice(&src[0..copy_len]);
            src = &src[copy_len..];
            if src.is_empty() {
                let pad_size = ESCAPE_LENGTH - 1 - copy_len;
                if pad_size > 0 {
                    dst.resize(dst.len() + pad_size, 0);
                }
                dst.push(copy_len as u8);
                break;
            }
            dst.push(ESCAPE_LENGTH as u8);
        }
    }

    pub fn decode(src: &mut &[u8], dst: &mut Vec<u8>) {
        loop {
            let extra = src[ESCAPE_LENGTH - 1];
            let len = cmp::min(ESCAPE_LENGTH - 1, extra as usize);
            dst.extend_from_slice(&src[..len]);
            // affected to the external original object
            *src = &src[ESCAPE_LENGTH..];
            if extra < ESCAPE_LENGTH as u8 {
                break;
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test() {
            let org1 = b"helloworld!memcmpable";
            let org2 = b"foobarbazhogehuga";

            let mut enc = vec![];
            encode(org1, &mut enc);
            encode(org2, &mut enc);

            let mut rest = &enc[..];

            let mut dec1 = vec![];
            decode(&mut rest, &mut dec1);
            assert_eq!(org1, dec1.as_slice());
            let mut dec2 = vec![];
            decode(&mut rest, &mut dec2);
            assert_eq!(org2, dec2.as_slice());
        }
    }
}
