
use rsths::ths::{THS};

fn main() {
    // 初始化日志
    // 创建 THS 实例
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    // 获取股票列表
    let klines = ths.block_data(0xE).expect("Failed to get klines");
    let rs = klines.payload.result;
    println!("板块代码: {:?}", rs);

    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 