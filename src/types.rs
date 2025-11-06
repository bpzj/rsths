use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

/// 多支实时 Tick 格式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    #[serde(rename(deserialize = "150"))]
    pub b4_p: f64, // 买4价
    #[serde(rename(deserialize = "151"))]
    pub b4_v: i64, // 买1量
    #[serde(rename(deserialize = "152"))]
    pub a4_p: f64, // 卖4价
    #[serde(rename(deserialize = "153"))]
    pub a4_v: i64, // 卖4量
    #[serde(rename(deserialize = "154"))]
    pub b5_p: f64, // 买5价
    #[serde(rename(deserialize = "155"))]
    pub b5_v: i64, // 买5量
    #[serde(rename(deserialize = "156"))]
    pub a5_p: f64, // 卖5价
    #[serde(rename(deserialize = "157"))]
    pub a5_v: i64, // 卖5量

    #[serde(rename(deserialize = "24"))]
    pub b1_p: f64, // 买1价
    #[serde(rename(deserialize = "25"))]
    pub b1_v: i64, // 买1量
    #[serde(rename(deserialize = "26"))]
    pub b2_p: f64, // 买2价
    #[serde(rename(deserialize = "27"))]
    pub b2_v: i64, // 买2量
    #[serde(rename(deserialize = "28"))]
    pub b3_p: f64, // 买3价
    #[serde(rename(deserialize = "29"))]
    pub b3_v: i64, // 买3量
    #[serde(rename(deserialize = "30"))]
    pub a1_p: f64, // 卖1价
    #[serde(rename(deserialize = "31"))]
    pub a1_v: i64, // 卖1量
    #[serde(rename(deserialize = "32"))]
    pub a2_p: f64, // 卖2价
    #[serde(rename(deserialize = "33"))]
    pub a2_v: i64, // 卖2量
    #[serde(rename(deserialize = "34"))]
    pub a3_p: f64, // 卖3价
    #[serde(rename(deserialize = "35"))]
    pub a3_v: i64, // 卖3量

    #[serde(rename(deserialize = "5"))]
    pub symbol: String,
    #[serde(rename(deserialize = "6"))]
    pub prev_close: f64, // 昨收价
}

/// 单支五档行情数据格式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickAll {
    /// 时间戳 北京时间 UTC+8
    #[serde(rename(deserialize = "1"))]
    pub time:i32,
    /// 价格
    #[serde(rename(deserialize = "10"))]
    pub price: f64,
    /// 成交方向   1   5   0   15 
    #[serde(rename(deserialize = "12"))]
    pub bs: u32,
    /// 总成交量
    #[serde(rename(deserialize = "13"))]
    pub volume: i32,
    /// 外盘成交量
    #[serde(rename(deserialize = "14"))]
    pub outer_volume: i32,
    /// 买4价
    #[serde(rename(deserialize = "150"))]
    pub b4_p: f64,
    /// 买1量
    #[serde(rename(deserialize = "151"))]
    pub b4_v: i64,
    /// 卖4价
    #[serde(rename(deserialize = "152"))]
    pub a4_p: f64,
    /// 卖4量
    #[serde(rename(deserialize = "153"))]
    pub a4_v: i64,
    /// 买5价
    #[serde(rename(deserialize = "154"))]
    pub b5_p: f64,
    /// 买5量
    #[serde(rename(deserialize = "155"))]
    pub b5_v: i64,
    /// 卖5价
    #[serde(rename(deserialize = "156"))]
    pub a5_p: f64,
    /// 卖5量
    #[serde(rename(deserialize = "157"))]
    pub a5_v: i64,

    /// 交易笔数
    #[serde(rename(deserialize = "18"))]
    pub trade_num: u32,
    /// 总成交额
    #[serde(rename(deserialize = "19"))]
    pub amount: i64,

    /// 买1价
    #[serde(rename(deserialize = "20"))]
    pub b1_p: f64,
    /// 卖1价
    #[serde(rename(deserialize = "21"))]
    pub a1_p: f64,
    /// 买1量
    #[serde(rename(deserialize = "25"))]
    pub b1_v: i64,
    /// 买2价
    #[serde(rename(deserialize = "26"))]
    pub b2_p: f64,
    /// 买2量
    #[serde(rename(deserialize = "27"))]
    pub b2_v: i64,
    /// 买3价
    #[serde(rename(deserialize = "28"))]
    pub b3_p: f64,
    /// 买3量
    #[serde(rename(deserialize = "29"))]
    pub b3_v: i64,

    /// 卖1量
    #[serde(rename(deserialize = "31"))]
    pub a1_v: i64,
    /// 卖2价
    #[serde(rename(deserialize = "32"))]
    pub a2_p: f64,
    /// 卖2量
    #[serde(rename(deserialize = "33"))]
    pub a2_v: i64,
    /// 卖3价
    #[serde(rename(deserialize = "34"))]
    pub a3_p: f64,
    /// 卖3量
    #[serde(rename(deserialize = "35"))]
    pub a3_v: i64,

    /// 当前量
    #[serde(rename(deserialize = "49"))]
    pub live_vol: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KLine {
    #[serde(skip_deserializing)]
    pub time: i64,
    #[serde(rename(deserialize = "1"),skip_serializing)]
    pub time_int: i32,
    /// 开盘价
    #[serde(rename(deserialize = "7"))]
    pub open: f64,
    /// 最高价
    #[serde(rename(deserialize = "8"))]
    pub high: f64,
    /// 最低价
    #[serde(rename(deserialize = "9"))]
    pub low: f64,
    /// 收盘价
    #[serde(rename(deserialize = "11"))]
    pub close: f64,
    /// 成交量
    #[serde(rename(deserialize = "13"))]
    pub volume: i64, 
    /// 成交额
    #[serde(rename(deserialize = "19"))]
    pub amount: f64, 
}

/// 订单簿
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThsOrderBook{
    pub orderlevel: i32,
    pub ordersque: Vec<i32>,
    pub price:f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub name: String,
    pub price: f64,
    pub change: f64,
    pub volume: i64,
    pub amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockData {
    #[serde(rename(deserialize = "5"))]
    pub symbol: String,
    #[serde(rename(deserialize = "55"))]
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    pub time: DateTime<Local>,
    pub price: f64,
    pub volume: i64,
    pub bs_flag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookData {
    pub price: f64,
    pub volume: i64,
    pub order_count: i32,
}

// 命名参考 https://quant.10jqka.com.cn/view/dataplatform/detail/20
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpoData {
    pub symbol: String,
    pub name: String,
    pub price: f64,
    pub pe: f64,
    pub shares: i64,
    pub date: String,
} 