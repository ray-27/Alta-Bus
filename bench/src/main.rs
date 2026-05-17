use protocol::messages::price_tick::PriceTick;
use protocol::{Encode, MsgHeader, MsgType};
use std::io::Write;
use std::net::TcpStream;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:7777").unwrap();

    let tick = PriceTick {
        instrument_id: 1001,
        last_price: 24150.50,
        volume: 500,
        total_traded_volume: 1_200_000,
        timestamp_ns: 1_700_000_000_000_000_000,
    };

    let payload = tick.encode();
    let header = MsgHeader::new(MsgType::PriceTick as u8, 1001, payload.len() as u32);

    stream.write_all(&header.to_bytes()).unwrap();
    stream.write_all(&payload).unwrap();

    println!("[bench] sent PriceTick on channel 1001");
}
