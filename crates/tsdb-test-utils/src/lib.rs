//! tsdb-test-utils -- 测试工具库
//!
//! 提供数据生成器、断言宏等测试辅助工具。

pub mod assertions;
pub mod data_gen;

pub use data_gen::*;
