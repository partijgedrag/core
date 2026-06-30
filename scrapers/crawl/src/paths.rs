use std::path::PathBuf;

/// Root directory for output data (e.g. parquet files).
/// Override with SCRAPER_DATA_DIR, defaults to "./data" for local dev.
pub fn data_dir() -> PathBuf {
    std::env::var("SCRAPER_DATA_DIR")
        .unwrap_or_else(|_| "data".to_string())
        .into()
}

/// Root directory for the HTML cache.
/// Override with SCRAPER_CACHE_DIR, defaults to "./scrapers/cache" for local dev.
pub fn cache_dir() -> PathBuf {
    std::env::var("SCRAPER_CACHE_DIR")
        .unwrap_or_else(|_| "scrapers/cache".to_string())
        .into()
}
