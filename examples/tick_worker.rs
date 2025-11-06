use rsths::ths::{THS, Adjust, Interval, ThsOption};
use std::env;

fn main() {
    let symbol = env::args().nth(1).expect("No symbol argument");
    let mut ths = THS::new(None).expect("Failed to create THS instance");
    ths.connect().expect("Failed to connect to server");
    let market_data = ths.tick_super_level1(&symbol)
        .expect("Failed to get market data");
    println!("五档 ({}): {:?}", symbol, market_data.payload.dict_extra);
    // ths.disconnect().expect("Failed to disconnect");
}