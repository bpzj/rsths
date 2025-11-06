
use rsths::ths::{THS, Adjust, Interval};
use chrono::{Local};

fn main() {
    // 初始化日志
    // 创建 THS 实例
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    let start_time = Local::now().timestamp_millis();
    // 获取股票列表
    let stocks = ths.order_book_bid("USHA600000").expect("Failed to get stock list");
    // println!("订单簿: {:?}", stocks);
    let end_time = Local::now().timestamp_millis();
    println!("{}", end_time - start_time);

    // 获取某只股票的K线数据
    let end_time = Local::now();
    // let start_time = end_time - chrono::Duration::days(7);
    
    let klines = ths.klines(
        "USZA300033",  // 浦发银行
        Some("2024-01-01 00:00:00"),
        Some("2025-01-01 00:00:00"),
        Adjust::NONE,
        Interval::DAY,
        0,
    ).expect("Failed to get klines");
    println!("K线数据: {:?}", klines);

    // 获取实时行情
    let market_data = ths.stock_market_data("USHA600000")
        .expect("Failed to get market data");
    println!("实时行情: {:?}", market_data);

    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 