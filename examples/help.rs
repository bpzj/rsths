
use rsths::ths::{THS};
use chrono::{Local};

fn main() {
    // 初始化日志
    // 创建 THS 实例
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    let start_time = Local::now().timestamp_millis();
    let help = ths.help("doc").expect("Failed to get stock list");
    println!("{}", help);
    let help = ths.help("about").expect("Failed to get stock list");
    println!("{}", help);

    let end_time = Local::now().timestamp_millis();
    println!("{}", end_time - start_time);


    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 