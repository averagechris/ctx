use std::{fs, io::Write, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

pub const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RefreshPolicy {
    Auto,
    Off,
    Strict,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigFile {
    search: SearchConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SearchConfig {
    refresh: Option<RefreshPolicy>,
}

pub fn read_refresh_policy(data_root: &Path) -> Result<Option<RefreshPolicy>> {
    let path = data_root.join(CONFIG_FILE);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("read ctx config {}", path.display()))
        }
    };
    let config = toml::from_str::<ConfigFile>(&contents)
        .with_context(|| format!("parse ctx config {}", path.display()))?;
    Ok(config.search.refresh)
}

pub fn write_default_config(data_root: &Path) -> Result<()> {
    let path = data_root.join(CONFIG_FILE);
    if path.exists() {
        return Ok(());
    }
    let mut file = fs::File::create(&path)?;
    file.write_all(
        b"# ctx configuration\n\n# Search refresh policy. Omit this section for the default `auto` behavior.\n# [search]\n# refresh = \"auto\"\n",
    )?;
    Ok(())
}
