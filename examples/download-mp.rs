use rsths::ths::{THS};
use std::process::Command;
use std::env;


/// 使用多个进程来加速
fn main() {
    // 如果是子进程，执行数据获取逻辑
    if let Ok(_download) = env::var("DOWNLOAD") {
        let thread_id = env::var("THREAD_ID").unwrap();
        download_child_process(&thread_id);
        return;
    }

    // 主进程逻辑：创建多个子进程
    let mut handles = vec![];
    for i in 0..10 {
        let mut child = Command::new(env::current_exe().unwrap())
            .env("THREAD_ID", i.to_string())
            .env("DOWNLOAD", "YES")
            .spawn()
            .expect("Failed to spawn process");

        handles.push(child);
    }

    // 等待所有子进程完成
    for mut child in handles {
        child.wait().expect("Failed to wait for child process");
    }
}

fn download_child_process(thread_id: &str) {
    // 在子进程中创建实例，不影响
    let mut ths = THS::new(None).expect("Failed to create instance");
    ths.connect().expect("Failed to connect to server");

    let klines = ths.tick_super_level1("USHA600795").expect("Failed to get tick in process");

    println!("Process {} got klines: {:?}", thread_id, klines.payload.result);

    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
}