
use rsths::ths::{THS};

/// 单个下载进程，
fn main() {

    // 创建 THS 实例
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    // 获取实时行情
    let market_data = ths.tick_super_level1("USHA600000")
        .expect("Failed to get market data");
    println!("五档: {:?}", market_data.payload.result);

    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 