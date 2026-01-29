//! Queue configuration for shard settings.

use serde::{Deserialize, Serialize};

/// Queue configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Number of hex characters for shard prefix (1-4).
    /// 1 = 16 shards, 2 = 256 shards, 3 = 4096 shards, 4 = 65536 shards.
    #[serde(default = "default_shard_prefix_len")]
    pub shard_prefix_len: usize,
}

const fn default_shard_prefix_len() -> usize {
    1
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            shard_prefix_len: 1,
        }
    }
}

impl QueueConfig {
    /// Generate all shard prefixes for this config.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn all_shards(&self) -> Vec<String> {
        let prefix_len = self.shard_prefix_len.clamp(1, 4);
        let count = 16_usize.pow(prefix_len as u32);
        (0..count).map(|i| format!("{i:0>prefix_len$x}")).collect()
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if `shard_prefix_len` is not between 1 and 4.
    pub fn validate(&self) -> Result<(), String> {
        if self.shard_prefix_len == 0 || self.shard_prefix_len > 4 {
            return Err(format!(
                "shard_prefix_len must be 1-4, got {}",
                self.shard_prefix_len
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = QueueConfig::default();
        assert_eq!(config.shard_prefix_len, 1);
    }

    #[test]
    fn test_all_shards_prefix_len_1() {
        let config = QueueConfig {
            shard_prefix_len: 1,
        };
        let shards = config.all_shards();
        assert_eq!(shards.len(), 16);
        assert_eq!(shards[0], "0");
        assert_eq!(shards[15], "f");
    }

    #[test]
    fn test_all_shards_prefix_len_2() {
        let config = QueueConfig {
            shard_prefix_len: 2,
        };
        let shards = config.all_shards();
        assert_eq!(shards.len(), 256);
        assert_eq!(shards[0], "00");
        assert_eq!(shards[255], "ff");
        assert_eq!(shards[16], "10");
    }

    #[test]
    fn test_all_shards_prefix_len_3() {
        let config = QueueConfig {
            shard_prefix_len: 3,
        };
        let shards = config.all_shards();
        assert_eq!(shards.len(), 4096);
        assert_eq!(shards[0], "000");
        assert_eq!(shards[4095], "fff");
    }

    #[test]
    fn test_all_shards_prefix_len_4() {
        let config = QueueConfig {
            shard_prefix_len: 4,
        };
        let shards = config.all_shards();
        assert_eq!(shards.len(), 65536);
        assert_eq!(shards[0], "0000");
        assert_eq!(shards[65535], "ffff");
    }

    #[test]
    fn test_all_shards_clamped_to_min() {
        let config = QueueConfig {
            shard_prefix_len: 0,
        };
        let shards = config.all_shards();
        // Should be clamped to 1
        assert_eq!(shards.len(), 16);
    }

    #[test]
    fn test_all_shards_clamped_to_max() {
        let config = QueueConfig {
            shard_prefix_len: 10,
        };
        let shards = config.all_shards();
        // Should be clamped to 4
        assert_eq!(shards.len(), 65536);
    }

    #[test]
    fn test_validate_valid() {
        let config = QueueConfig {
            shard_prefix_len: 2,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_too_small() {
        let config = QueueConfig {
            shard_prefix_len: 0,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_too_large() {
        let config = QueueConfig {
            shard_prefix_len: 5,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_serialization() {
        let config = QueueConfig {
            shard_prefix_len: 2,
        };
        let json = serde_json::to_string(&config).expect("serialize");
        let deserialized: QueueConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.shard_prefix_len, 2);
    }

    #[test]
    fn test_deserialization_with_default() {
        let json = "{}";
        let config: QueueConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(config.shard_prefix_len, 1);
    }
}
