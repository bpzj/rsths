use std::thread::sleep;
use std::time;
use rsths::ths::{THS, Adjust, Interval};
use chrono::{Local, Timelike};

fn main() {
    // 初始化日志
    // 创建 THS 实例
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    loop {
        let start_time = Local::now().timestamp_millis();
        // 获取股票列表
        let stocks = ths.order_book_bid("USHA600588").expect("Failed to get stock list");
        // println!("订单簿: {:?}", stocks.payload.result);

        let stocks = ths.order_book_ask("USHA600588").expect("Failed to get stock list");
        // println!("订单簿: {:?}", stocks.payload.result);
        let end_time = Local::now().timestamp_millis();
        println!("{}", end_time - start_time);
        // println!("订单簿: {:?}", stocks);
        // 限制频率 500ms
        sleep(time::Duration::from_millis((515 - (end_time - start_time)) as u64));
    }
    // 获取股票列表


    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 