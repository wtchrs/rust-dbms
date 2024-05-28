use bincode::Options;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Pair<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Pair<'a> {
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::options().serialize(&self).unwrap()
    }

    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        bincode::options().deserialize(bytes).unwrap()
    }
}
