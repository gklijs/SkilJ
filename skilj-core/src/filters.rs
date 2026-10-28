//! valid_filters and matches_filters.
//!
//! The spec leaves the operator/type matrix to this black box. Ours:
//!
//! | operator      | applies to                                  | value encoding               |
//! |---------------|---------------------------------------------|------------------------------|
//! | equals        | any scalar                                  | the scalar, as a string      |
//! | contains      | string (substring), list (element equals)   | the scalar                   |
//! | is_like       | string                                      | SQL LIKE pattern (`%`, `_`)  |
//! | greater_than  | integer, number, string with date/date-time | the scalar                   |
//! | less_than     | as greater_than                             | the scalar                   |
//! | in            | any scalar                                  | JSON array of candidates     |
//! | near          | string, format geo-point ("lat,lon")        | "lat,lon,km"                 |
//! | similar_color | string, format color ("#rrggbb")            | "#rrggbb,max_distance(0-441)"|
//! | in_subnet     | string, format ip/ipv4/ipv6                 | CIDR                         |

use std::net::IpAddr;

use ipnet::IpNet;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::model::{Config, Filter, FilterOperator};
use crate::schema::{read_path, Leaf, ScalarType, Schema};
use crate::tags::scalar_tag_value;

/// A filter checked against a schema and ready to evaluate.
#[derive(Debug, Clone)]
pub struct CompiledFilter {
    pub filter: Filter,
    leaf: Leaf,
    pattern: Option<regex::Regex>,
    candidates: Vec<Value>,
    geo: Option<(f64, f64, f64)>,
    color: Option<((u8, u8, u8), f64)>,
    net: Option<IpNet>,
}

fn is_geo(format: Option<&str>) -> bool {
    matches!(format, Some("geo-point") | Some("geo"))
}
fn is_color(format: Option<&str>) -> bool {
    matches!(format, Some("color") | Some("colour"))
}
fn is_ip(format: Option<&str>) -> bool {
    matches!(format, Some("ip") | Some("ipv4") | Some("ipv6"))
}

/// valid_filters(event_type, filters).
pub fn compile_filters(
    schema: &Schema,
    filters: &[Filter],
    config: &Config,
) -> Result<Vec<CompiledFilter>> {
    if filters.len() > config.max_filters {
        return Err(Error::invalid(format!(
            "at most {} filters",
            config.max_filters
        )));
    }
    filters.iter().map(|f| compile(schema, f, config)).collect()
}

fn compile(schema: &Schema, f: &Filter, config: &Config) -> Result<CompiledFilter> {
    let leaf = schema.resolve(&f.field)?;
    let limit = if f.operator == FilterOperator::IsLike {
        config.max_filter_value_len / 4
    } else {
        config.max_filter_value_len
    };
    if f.value.len() > limit {
        return Err(Error::invalid(format!(
            "filter value on '{}' is too long",
            f.field
        )));
    }
    let scalar = leaf.scalar().clone();
    let is_list = matches!(leaf, Leaf::ListOfScalars(_));
    let bad = || {
        Error::invalid(format!(
            "operator {:?} does not apply to field '{}' ({}{})",
            f.operator,
            f.field,
            if is_list { "list of " } else { "" },
            scalar.ty.name()
        ))
    };
    let mut c = CompiledFilter {
        filter: f.clone(),
        leaf: leaf.clone(),
        pattern: None,
        candidates: vec![],
        geo: None,
        color: None,
        net: None,
    };
    let fmt = scalar.format.as_deref();
    match f.operator {
        FilterOperator::Equals => {
            parse_scalar(scalar.ty, &f.value)?;
        }
        FilterOperator::Contains => {
            if !(is_list || scalar.ty == ScalarType::String) {
                return Err(bad());
            }
        }
        FilterOperator::IsLike => {
            if is_list || scalar.ty != ScalarType::String {
                return Err(bad());
            }
            c.pattern = Some(like_to_regex(&f.value)?);
        }
        FilterOperator::GreaterThan | FilterOperator::LessThan => {
            let ordered = matches!(scalar.ty, ScalarType::Integer | ScalarType::Number)
                || (scalar.ty == ScalarType::String
                    && matches!(fmt, Some("date") | Some("date-time")));
            if is_list || !ordered {
                return Err(bad());
            }
            parse_scalar(scalar.ty, &f.value)?;
        }
        FilterOperator::In => {
            if is_list {
                return Err(bad());
            }
            let v: Value = serde_json::from_str(&f.value).map_err(|_| {
                Error::invalid("an 'in' filter value is a JSON array of candidates")
            })?;
            let arr = v.as_array().ok_or_else(|| {
                Error::invalid("an 'in' filter value is a JSON array of candidates")
            })?;
            if arr.is_empty() || arr.len() > 100 {
                return Err(Error::invalid("an 'in' filter takes 1 to 100 candidates"));
            }
            for x in arr {
                if !scalar.ty.accepts(x) {
                    return Err(Error::invalid(format!(
                        "'in' candidate {x} is not a {}",
                        scalar.ty.name()
                    )));
                }
            }
            c.candidates = arr.clone();
        }
        FilterOperator::Near => {
            if is_list || scalar.ty != ScalarType::String || !is_geo(fmt) {
                return Err(bad());
            }
            let parts: Vec<f64> = f
                .value
                .split(',')
                .map(|p| p.trim().parse::<f64>())
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| Error::invalid("near takes \"lat,lon,km\""))?;
            if parts.len() != 3 || parts[2] < 0.0 {
                return Err(Error::invalid("near takes \"lat,lon,km\""));
            }
            c.geo = Some((parts[0], parts[1], parts[2]));
        }
        FilterOperator::SimilarColor => {
            if is_list || scalar.ty != ScalarType::String || !is_color(fmt) {
                return Err(bad());
            }
            let (hex, dist) = f
                .value
                .split_once(',')
                .ok_or_else(|| Error::invalid("similar_color takes \"#rrggbb,distance\""))?;
            let rgb = parse_color(hex.trim()).ok_or_else(|| Error::invalid("bad colour"))?;
            let d: f64 = dist
                .trim()
                .parse()
                .map_err(|_| Error::invalid("bad colour distance"))?;
            c.color = Some((rgb, d));
        }
        FilterOperator::InSubnet => {
            if is_list || scalar.ty != ScalarType::String || !is_ip(fmt) {
                return Err(bad());
            }
            c.net = Some(
                f.value
                    .trim()
                    .parse()
                    .map_err(|_| Error::invalid("in_subnet takes a CIDR"))?,
            );
        }
    }
    Ok(c)
}

fn parse_scalar(ty: ScalarType, s: &str) -> Result<Value> {
    Ok(match ty {
        ScalarType::String => Value::String(s.to_string()),
        ScalarType::Integer => Value::from(
            s.parse::<i64>()
                .map_err(|_| Error::invalid(format!("'{s}' is not an integer")))?,
        ),
        ScalarType::Number => serde_json::Number::from_f64(
            s.parse::<f64>()
                .map_err(|_| Error::invalid(format!("'{s}' is not a number")))?,
        )
        .map(Value::Number)
        .ok_or_else(|| Error::invalid("not a finite number"))?,
        ScalarType::Boolean => Value::Bool(
            s.parse::<bool>()
                .map_err(|_| Error::invalid(format!("'{s}' is not a boolean")))?,
        ),
    })
}

fn like_to_regex(p: &str) -> Result<regex::Regex> {
    let mut re = String::from("^");
    for ch in p.chars() {
        match ch {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            c => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    re.push('$');
    regex::RegexBuilder::new(&re)
        .size_limit(1 << 16)
        .build()
        .map_err(|e| Error::invalid(format!("bad is_like pattern: {e}")))
}

fn parse_color(s: &str) -> Option<(u8, u8, u8)> {
    let h = s.strip_prefix('#')?;
    if h.len() != 6 {
        return None;
    }
    Some((
        u8::from_str_radix(&h[0..2], 16).ok()?,
        u8::from_str_radix(&h[2..4], 16).ok()?,
        u8::from_str_radix(&h[4..6], 16).ok()?,
    ))
}

fn haversine_km(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (lat1, lon1) = (a.0.to_radians(), a.1.to_radians());
    let (lat2, lon2) = (b.0.to_radians(), b.1.to_radians());
    let h = ((lat2 - lat1) / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * ((lon2 - lon1) / 2.0).sin().powi(2);
    2.0 * 6371.0 * h.sqrt().asin()
}

fn compare(a: &Value, b: &str, ty: ScalarType) -> Option<std::cmp::Ordering> {
    match ty {
        ScalarType::Integer | ScalarType::Number => {
            a.as_f64()?.partial_cmp(&b.parse::<f64>().ok()?)
        }
        ScalarType::String => {
            let a = a.as_str()?;
            match (
                chrono::DateTime::parse_from_rfc3339(a),
                chrono::DateTime::parse_from_rfc3339(b),
            ) {
                (Ok(x), Ok(y)) => Some(x.cmp(&y)),
                _ => Some(a.cmp(b)),
            }
        }
        ScalarType::Boolean => None,
    }
}

impl CompiledFilter {
    pub fn matches(&self, payload: &Value) -> bool {
        let Some(v) = read_path(payload, &self.filter.field) else {
            return false;
        };
        let ty = self.leaf.scalar().ty;
        let want = &self.filter.value;
        match self.filter.operator {
            FilterOperator::Equals => match v {
                Value::Array(items) => items
                    .iter()
                    .any(|i| scalar_tag_value(i).as_deref() == Some(want)),
                other => scalar_eq(other, want, ty),
            },
            FilterOperator::Contains => match v {
                Value::Array(items) => items
                    .iter()
                    .any(|i| scalar_tag_value(i).as_deref() == Some(want)),
                Value::String(s) => s.contains(want.as_str()),
                _ => false,
            },
            FilterOperator::IsLike => v
                .as_str()
                .is_some_and(|s| self.pattern.as_ref().is_some_and(|p| p.is_match(s))),
            FilterOperator::GreaterThan => {
                compare(v, want, ty) == Some(std::cmp::Ordering::Greater)
            }
            FilterOperator::LessThan => compare(v, want, ty) == Some(std::cmp::Ordering::Less),
            FilterOperator::In => self.candidates.iter().any(|c| values_equal(c, v)),
            FilterOperator::Near => {
                let Some((lat, lon, km)) = self.geo else {
                    return false;
                };
                v.as_str()
                    .and_then(|s| {
                        let (a, b) = s.split_once(',')?;
                        Some((a.trim().parse::<f64>().ok()?, b.trim().parse::<f64>().ok()?))
                    })
                    .is_some_and(|p| haversine_km(p, (lat, lon)) <= km)
            }
            FilterOperator::SimilarColor => {
                let Some(((r, g, b), d)) = self.color else {
                    return false;
                };
                v.as_str()
                    .and_then(parse_color)
                    .is_some_and(|(r2, g2, b2)| {
                        let dist = ((r as f64 - r2 as f64).powi(2)
                            + (g as f64 - g2 as f64).powi(2)
                            + (b as f64 - b2 as f64).powi(2))
                        .sqrt();
                        dist <= d
                    })
            }
            FilterOperator::InSubnet => {
                let Some(net) = self.net else { return false };
                v.as_str()
                    .and_then(|s| s.parse::<IpAddr>().ok())
                    .is_some_and(|ip| net.contains(&ip))
            }
        }
    }
}

fn scalar_eq(v: &Value, want: &str, ty: ScalarType) -> bool {
    match ty {
        ScalarType::Integer | ScalarType::Number => match (v.as_f64(), want.parse::<f64>()) {
            (Some(a), Ok(b)) => a == b,
            _ => false,
        },
        _ => scalar_tag_value(v).as_deref() == Some(want),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

pub fn matches_filters(filters: &[CompiledFilter], payload: &Value) -> bool {
    filters.iter().all(|f| f.matches(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> Schema {
        Schema::parse(&json!({"type":"object","properties":{
            "name":{"type":"string"},
            "amount":{"type":"integer"},
            "ip":{"type":"string","format":"ipv4"},
            "loc":{"type":"string","format":"geo-point"},
            "color":{"type":"string","format":"color"},
            "tags":{"type":"array","items":{"type":"string"}}
        }}))
        .unwrap()
    }

    fn f(field: &str, op: FilterOperator, value: &str) -> Filter {
        Filter {
            field: field.into(),
            operator: op,
            value: value.into(),
        }
    }

    #[test]
    fn matrix() {
        let s = schema();
        let c = Config::default();
        assert!(compile_filters(&s, &[f("name", FilterOperator::GreaterThan, "a")], &c).is_err());
        assert!(compile_filters(&s, &[f("amount", FilterOperator::IsLike, "a")], &c).is_err());
        assert!(
            compile_filters(&s, &[f("name", FilterOperator::InSubnet, "10.0.0.0/8")], &c).is_err()
        );
        let fs = compile_filters(
            &s,
            &[
                f("amount", FilterOperator::GreaterThan, "10"),
                f("ip", FilterOperator::InSubnet, "10.0.0.0/8"),
                f("name", FilterOperator::IsLike, "Jo%"),
                f("tags", FilterOperator::Contains, "vip"),
                f("loc", FilterOperator::Near, "52.37,4.89,10"),
                f("color", FilterOperator::SimilarColor, "#ff0000,30"),
            ],
            &c,
        )
        .unwrap();
        let p = json!({"name":"John","amount":11,"ip":"10.1.2.3","tags":["vip"],"loc":"52.36,4.90","color":"#f00a0a"});
        assert!(matches_filters(&fs, &p));
        let p2 = json!({"name":"John","amount":9,"ip":"10.1.2.3","tags":["vip"],"loc":"52.36,4.90","color":"#f00a0a"});
        assert!(!matches_filters(&fs, &p2));
    }
}
