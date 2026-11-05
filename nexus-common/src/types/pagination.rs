use serde::de::{self, Deserializer};
use serde::Deserialize;
use utoipa::ToSchema;

/// Largest page a caller can request from a public list endpoint. A larger
/// `limit` is served as this many items instead of sizing Redis ranges,
/// hydration and response buffers by the caller's number.
pub const MAX_PAGE_LIMIT: usize = 1_000;

/// `requested`, or `default` when absent, never above [MAX_PAGE_LIMIT].
pub fn bounded_page_limit(requested: Option<usize>, default: usize) -> usize {
    requested.unwrap_or(default).min(MAX_PAGE_LIMIT)
}

#[derive(Default, Deserialize, Debug, ToSchema, Clone)]
pub struct Pagination {
    #[serde(default, deserialize_with = "parse_string_to_usize")]
    pub skip: Option<usize>,
    #[serde(default, deserialize_with = "parse_string_to_usize")]
    pub limit: Option<usize>,
    #[serde(default, deserialize_with = "parse_string_to_f64")]
    pub start: Option<f64>,
    #[serde(default, deserialize_with = "parse_string_to_f64")]
    pub end: Option<f64>,
}

impl Pagination {
    /// The requested page size, `default` when absent, never above [MAX_PAGE_LIMIT].
    pub fn bounded_limit(&self, default: usize) -> usize {
        bounded_page_limit(self.limit, default)
    }
}

// Parse a string into a usize
fn parse_string_to_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => s.parse::<usize>().map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

// Parsing strings or floats into f64
fn parse_string_to_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => s.parse::<f64>().map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_limit_uses_the_default() {
        assert_eq!(Pagination::default().bounded_limit(200), 200);
        assert_eq!(bounded_page_limit(None, 5), 5);
    }

    #[test]
    fn a_limit_inside_the_bound_is_kept() {
        let page = Pagination {
            limit: Some(MAX_PAGE_LIMIT),
            ..Default::default()
        };
        assert_eq!(page.bounded_limit(20), MAX_PAGE_LIMIT);
        assert_eq!(bounded_page_limit(Some(3), 5), 3);
    }

    #[test]
    fn a_limit_above_the_bound_is_clamped() {
        let page = Pagination {
            limit: Some(usize::MAX),
            ..Default::default()
        };
        assert_eq!(page.bounded_limit(20), MAX_PAGE_LIMIT);
        assert_eq!(
            bounded_page_limit(Some(MAX_PAGE_LIMIT + 1), 5),
            MAX_PAGE_LIMIT
        );
    }

    #[test]
    fn a_default_above_the_bound_is_clamped_too() {
        assert_eq!(bounded_page_limit(None, usize::MAX), MAX_PAGE_LIMIT);
    }
}
