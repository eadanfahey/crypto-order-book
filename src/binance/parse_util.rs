use std::str::from_utf8;

use serde::{Deserialize, Deserializer};

use super::errors::Error;

/// Parses a decimal number represented as a string to a scaled `u64`.
/// Example: parse_scaled_number("24765.230000", 2) -> 2476523.
pub(crate) fn parse_scaled_number(s: &str, scale: u32) -> u64 {
    let dot_idx = s.find('.');
    if let Some(i) = dot_idx {
        let whole = s[..i].parse::<u64>().unwrap();
        let fractional = if scale > 0 {
            let start = i + 1;
            let end = std::cmp::min(start + scale as usize, s.len());
            let part = &s[start..end];
            let mut fractional = part.parse::<u64>().unwrap();
            if part.len() < scale as usize {
                let d = scale - part.len() as u32;
                fractional *= (10 as u64).pow(d);
            }
            fractional
        } else {
            0
        };
        if whole == 0 && fractional == 0 {
            return 0;
        }
        return whole * (10 as u64).pow(scale) + fractional;
    } else {
        return s.parse::<u64>().unwrap() * (10 as u64).pow(scale);
    }
}

pub(crate) fn deserialize_decimal_e8<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    Ok(parse_scaled_number(s, 8))
}

pub(crate) fn parse_int(s: &[u8]) -> Result<u64, Error> {
    from_utf8(s)
        .map_err(|_| Error::ParseError("could not read utf8".to_owned()))
        .and_then(|s| {
            s.parse::<u64>()
                .map_err(|_| Error::ParseError("could not parse integer".to_owned()))
        })
}

#[cfg(test)]
mod tests {
    use super::parse_scaled_number;

    #[test]
    fn test_parse_scaled_number() {
        assert_eq!(parse_scaled_number("25700.51000000", 2), 2570051);
        assert_eq!(parse_scaled_number("25700.51000000", 4), 257005100);
        assert_eq!(parse_scaled_number("25700.51000000", 0), 25700);
        assert_eq!(parse_scaled_number("25700.51000000", 1), 257005);
        assert_eq!(parse_scaled_number("0.00000000", 1), 0);
        assert_eq!(parse_scaled_number("0.00000000", 8), 0);
        assert_eq!(parse_scaled_number("0.00064000", 8), 64000);
        assert_eq!(parse_scaled_number("0.00064000", 2), 0);
        assert_eq!(parse_scaled_number("0.00064000", 10), 6400000);
    }
}
