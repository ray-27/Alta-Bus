mod ring;

use protocol::{HEADER_SIZE, MsgHeader};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    let listner = TcpListener::bind("127.0.0.1:7777").expect("failed to bind to port");
    println!("[bus] listining on 127.0.0.1:7777");

    for stream in listner.incoming() {
        match stream {
            Ok(stream) => {
                let addr = stream.peer_addr().unwrap();
                println!("[bus] new connection from {}", addr);
                thread::spawn(move || handle_connection(stream));
            }
            Err(e) => eprintln!("[bus] accept error: {}", e),
        }
    }
}

fn handle_connection(mut stream: TcpStream) {
    let addr = stream.peer_addr().unwrap();
    let mut header_buf = [0u8; HEADER_SIZE];

    loop {
        //read exactly 17 bytes from the header, as thats the header size
        match stream.read_exact(&mut header_buf) {
            Ok(_) => {}
            Err(_) => {
                println!("[bus] {} disconnected", addr);
                return
            }
        }

        let header = match MsgHeader::from_bytes(&header_buf) {
            Some(h) => h,
            None => {
                eprintln!("[bus] malformed header from {}", addr);
                return;
            }
        };

        let mut payload = vec![0u8; header.payload_len as usize];
        if let Err(e) = stream.read_exact(&mut payload) {
            eprintln!("[bus] failed to read payload from {}: {}", addr,e);
            return;
        }

        println!(
            "[bus] recieved | msg_type={} channel_id={} payload_len={}",
            header.msg_type, header.channel_id, header.payload_len
        );
    }
}
