use crate::config::Config;
use crate::error::Result;

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

pub(crate) struct DiskStatistics {
    data_dir: PathBuf,
    used: u64,
    available: u64,
    last_update: Instant,
    update_interval: Duration,
}

impl DiskStatistics {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let mut instance = Self {
            data_dir: config.datanode.data_dir.clone(),
            used: 0,
            available: 0,
            last_update: Instant::now(),
            update_interval: Duration::from_secs(config.datanode.disk_statistics_interval),
        };
        instance.refresh(true)?;

        Ok(instance)
    }

    fn refresh(&mut self, force: bool) -> Result<()> {
        if force || Instant::now().duration_since(self.last_update) > self.update_interval {
            let output = Command::new("df")
                .arg("-k")
                .arg("--output=used,avail")
                .arg(&self.data_dir)
                .output()?;

            // TODO: There are a lot of unwraps, proper error handling would be better
            let output = String::from_utf8_lossy(&output.stdout);
            let mut lines = output.lines();
            lines.next(); // skip header
            let line = lines.next().unwrap();
            let mut columns = line.split_whitespace();
            self.used = columns.next().unwrap().parse().unwrap();
            self.available = columns.next().unwrap().parse().unwrap();
            self.last_update = Instant::now();
        }
        Ok(())
    }

    pub(crate) fn used(&mut self) -> Result<u64> {
        self.refresh(false)?;
        Ok(self.used)
    }

    pub(crate) fn available(&mut self) -> Result<u64> {
        self.refresh(false)?;
        Ok(self.available)
    }
}

#[cfg(test)]
mod tests {
    use super::DiskStatistics;
    use crate::config::Config;

    use std::path::PathBuf;

    #[test]
    fn returns_something() {
        let mut config = Config::default();
        config.datanode.data_dir = PathBuf::from("/tmp");

        let mut statistics =
            DiskStatistics::new(&config).expect("Should be able to create an instance");

        statistics
            .used()
            .expect("Should return how many kB are used");

        statistics
            .available()
            .expect("Should return how many kB are available");
    }
}
