pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Option<Self>;
}
