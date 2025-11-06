
use rsths::ths::{THS, Adjust, Interval, ThsOption};

fn main() {

    // 不能在一个进程中创建ths，需要创建多个进程，而且不能是work进程模式
    let mut ths = THS::new(None).expect("Failed to create THS instance");

    // 连接到服务器
    ths.connect().expect("Failed to connect to server");

    // 获取实时行情
    let market_data = ths.tick_super_level1("USHA600795")
        .expect("Failed to get market data");
    println!("五档: {:?}", market_data.payload.result.get(0));

    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
} 