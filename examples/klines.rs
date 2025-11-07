use parquet::{arrow::ArrowWriter, basic::{Compression, ZstdLevel}, file::properties::WriterProperties};
use rsths::{ths::{Adjust, Interval, THS}, types::KLine};
use std::fs::File;
use arrow::array::{TimestampMillisecondArray, Float64Array, Int64Array};
use arrow::datatypes::{Field, DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;


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
    // 把rs写入到 parquet 文件
    write_kline_to_parquet(rs, "USHA600795.parquet").expect("Failed to write klines to parquet");

    // 断开连接
    ths.disconnect().expect("Failed to disconnect");
}

pub fn write_kline_to_parquet(kline_data: Vec<KLine>, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    if kline_data.is_empty() {
        return Ok(());
    }

    let len: usize = kline_data.len();
    
    // 准备数据数组
    let mut time_data = Vec::with_capacity(len);
    let mut open_data = Vec::with_capacity(len);
    let mut high_data = Vec::with_capacity(len);
    let mut low_data = Vec::with_capacity(len);
    let mut close_data = Vec::with_capacity(len);
    let mut volume_data = Vec::with_capacity(len);
    let mut amount_data = Vec::with_capacity(len);

    for kline in kline_data {
        time_data.push(kline.time*1000); // 转换为毫秒
        open_data.push(kline.open);
        high_data.push(kline.high);
        low_data.push(kline.low);
        close_data.push(kline.close);
        volume_data.push(kline.volume);
        amount_data.push(kline.amount);
    }

    // 创建 Arrow 数组
    let time_array = TimestampMillisecondArray::from(time_data);
    let open_array = Float64Array::from(open_data);
    let high_array = Float64Array::from(high_data);
    let low_array = Float64Array::from(low_data);
    let close_array = Float64Array::from(close_data);
    let volume_array = Int64Array::from(volume_data);
    let amount_array = Float64Array::from(amount_data);

    // 定义 schema
    let schema = Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]);

    // 创建 RecordBatch
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(time_array),
            Arc::new(open_array),
            Arc::new(high_array),
            Arc::new(low_array),
            Arc::new(close_array),
            Arc::new(volume_array),
            Arc::new(amount_array),
        ],
    )?;

    // Create a Parquet file writer with ZSTD compression
    let file = File::create(filename)?;
    // 配置 ZSTD 压缩 - 修正写法
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?)) // 这里修正
        .set_dictionary_enabled(true)   // 启用字典编码
        .set_write_batch_size(1024)     // 设置批处理大小
        .build();
    // 使用 ArrowWriter 而不是 ParquetFileWriter
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;


    // Write the batch to the Parquet file
    writer.write(&batch)?;
    writer.finish()?;

    println!("成功写入 {} 条K线数据到 {}", len, filename);
    Ok(())
}