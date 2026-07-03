use std::{fs, io::Write, path::Path};

use anyhow::Result;

pub const CONFIG_FILE: &str = "config.toml";

pub fn write_default_config(data_root: &Path) -> Result<()> {
    let path = data_root.join(CONFIG_FILE);
    if path.exists() {
        return Ok(());
    }
    let mut file = fs::File::create(&path)?;
    file.write_all(b"# ctx configuration\n")?;
    Ok(())
}
