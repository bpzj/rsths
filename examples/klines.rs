
use rsths::ths::{THS, Adjust, Interval};
use chrono::{Local};
use csv;

fn main() {
    // 初始化日志
    // 创建 THS 实例
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    // 获取股票列表
    // 获取某只股票的K线数据
    // let start_time = end_time - chrono::Duration::days(7);
    
    let klines = ths.klines(
        "USHA600795",  // 国电电力
        Some("2024-01-01 00:00:00"),
        Some("2025-01-01 00:00:00"),
        Adjust::FORWARD,
        Interval::MIN_5,
        1000,
    ).expect("Failed to get klines");
    let rs = klines.payload.result;
    let mut wtr = csv::Writer::from_path("klines.csv").expect("Failed to create CSV file");
    for row in rs {
        wtr.serialize(row).expect("Failed to write row");
    }
    wtr.flush().expect("Failed to flush CSV writer");
    // println!("K线数据: {:?}", klines);


    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 