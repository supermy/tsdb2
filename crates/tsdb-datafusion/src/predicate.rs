use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ExtractedFilters {
    pub time_range: Option<(i64, i64)>,
    pub tag_filters: HashMap<String, String>,
}

pub fn extract_filters(filters: &[Expr]) -> ExtractedFilters {
    extract_filters_with_schema(filters, None)
}

pub fn extract_filters_with_schema(
    filters: &[Expr],
    schema: Option<arrow::datatypes::SchemaRef>,
) -> ExtractedFilters {
    let mut time_range: Option<(i64, i64)> = None;
    let mut tag_filters: HashMap<String, String> = HashMap::new();

    for expr in filters {
        extract_from_expr(expr, &mut time_range, &mut tag_filters, schema.as_ref());
    }

    ExtractedFilters {
        time_range,
        tag_filters,
    }
}

fn extract_from_expr(
    expr: &Expr,
    time_range: &mut Option<(i64, i64)>,
    tag_filters: &mut HashMap<String, String>,
    schema: Option<&Arc<arrow::datatypes::Schema>>,
) {
    if let Expr::BinaryExpr(binary) = expr {
        match binary.op {
            Operator::And => {
                extract_from_expr(&binary.left, time_range, tag_filters, schema);
                extract_from_expr(&binary.right, time_range, tag_filters, schema);
            },
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq => {
                try_extract_comparison(
                    &binary.left,
                    &binary.right,
                    &binary.op,
                    time_range,
                    tag_filters,
                    schema,
                );
            },
            _ => {},
        }
    }
}

fn try_extract_comparison(
    left: &Expr,
    right: &Expr,
    op: &Operator,
    time_range: &mut Option<(i64, i64)>,
    tag_filters: &mut HashMap<String, String>,
    schema: Option<&Arc<arrow::datatypes::Schema>>,
) {
    let (col_expr, val_expr, flipped) = match (left, right) {
        (Expr::Column(_), Expr::Literal(_)) => (left, right, false),
        (Expr::Literal(_), Expr::Column(_)) => (right, left, true),
        _ => return,
    };

    let col_name = match col_expr {
        Expr::Column(c) => c.name(),
        _ => return,
    };

    let scalar = match val_expr {
        Expr::Literal(s) => s,
        _ => return,
    };

    match col_name {
        "timestamp" => {
            let ts_micros = scalar_to_i64(scalar);
            if let Some(ts) = ts_micros {
                let effective_op = if flipped {
                    match op {
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        other => *other,
                    }
                } else {
                    *op
                };

                match effective_op {
                    Operator::Gt => {
                        if let Some((start, _)) = time_range {
                            *start = (*start).max(ts + 1);
                        } else {
                            *time_range = Some((ts + 1, i64::MAX));
                        }
                    },
                    Operator::GtEq => {
                        if let Some((start, _)) = time_range {
                            *start = (*start).max(ts);
                        } else {
                            *time_range = Some((ts, i64::MAX));
                        }
                    },
                    Operator::Lt => {
                        if let Some((_, end)) = time_range {
                            *end = (*end).min(ts - 1);
                        } else {
                            *time_range = Some((0, ts - 1));
                        }
                    },
                    Operator::LtEq => {
                        if let Some((_, end)) = time_range {
                            *end = (*end).min(ts);
                        } else {
                            *time_range = Some((0, ts));
                        }
                    },
                    Operator::Eq => {
                        *time_range = Some((ts, ts));
                    },
                    _ => {},
                }
            }
        },
        name => {
            let is_tag = if let Some(s) = schema {
                s.field_with_name(name)
                    .map(|f| {
                        f.metadata()
                            .get("tsdb_role")
                            .map(|r| r == "tag")
                            .unwrap_or_else(|| {
                                name.starts_with("tag_")
                                    && f.data_type() == &arrow::datatypes::DataType::Utf8
                            })
                    })
                    .unwrap_or(false)
            } else {
                name.starts_with("tag_")
            };
            if is_tag {
                if let Operator::Eq = op {
                    if let ScalarValue::Utf8(Some(val)) = scalar {
                        let tag_key = name.strip_prefix("tag_").unwrap_or(name).to_string();
                        tag_filters.insert(tag_key, val.clone());
                    }
                }
            }
        },
    }
}

fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(*v * 1000),
        ScalarValue::TimestampSecond(Some(v), _) => Some(*v * 1_000_000),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Utf8(Some(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn test_extract_timestamp_range() {
        let filters =
            vec![col("timestamp").gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1000), None)))];
        let result = extract_filters(&filters);
        assert_eq!(result.time_range, Some((1000, i64::MAX)));
    }

    #[test]
    fn test_extract_tag_filter() {
        let filters = vec![col("tag_host").eq(lit("server01"))];
        let result = extract_filters(&filters);
        assert_eq!(
            result.tag_filters.get("host"),
            Some(&"server01".to_string())
        );
    }

    #[test]
    fn test_extract_combined() {
        let ts_filter =
            col("timestamp").gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1000), None)));
        let tag_filter = col("tag_host").eq(lit("server01"));
        let combined = ts_filter.and(tag_filter);

        let result = extract_filters(&[combined]);
        assert_eq!(result.time_range, Some((1000, i64::MAX)));
        assert_eq!(
            result.tag_filters.get("host"),
            Some(&"server01".to_string())
        );
    }
}
