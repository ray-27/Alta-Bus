use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct ChannelConfig {
    pub price_ticks: HashMap<u32, String>,
    pub order_book: HashMap<u32, String>,
    pub signals: HashMap<u32, String>,
    pub system: HashMap<u32, String>,
}

impl ChannelConfig {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: ChannelConfig = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Look up the name for any channel_id across all groups.
    pub fn name_for(&self, channel_id: u32) -> Option<&str> {
        self.price_ticks
            .get(&channel_id)
            .or_else(|| self.order_book.get(&channel_id))
            .or_else(|| self.signals.get(&channel_id))
            .or_else(|| self.system.get(&channel_id))
            .map(|s| s.as_str())
    }

    /// All channel IDs across every group.
    pub fn all_channel_ids(&self) -> Vec<u32> {
        self.price_ticks
            .keys()
            .chain(self.order_book.keys())
            .chain(self.signals.keys())
            .chain(self.system.keys())
            .copied()
            .collect()
    }
}
