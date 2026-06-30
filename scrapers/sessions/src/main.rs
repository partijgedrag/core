use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crawl::paths::{cache_dir, data_dir};
use parquet::arrow::ArrowWriter;
use scraper::{Html, Selector};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::fs;

static SEL_SESSION: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("div a[href*='showpage.cfm']").unwrap());

#[derive(Debug)]
struct ScrapedSession {
    session_id: String,
    start_date: String,
    end_date: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let index_cache = cache_dir().join("sessions/index.html");
    fs::create_dir_all(index_cache.parent().unwrap()).await?;

    let parquet_path = data_dir().join("sessions.parquet");
    fs::create_dir_all(parquet_path.parent().unwrap()).await?;

    let url = "https://www.dekamer.be/kvvcr/showpage.cfm?section=/depute&language=nl&cfm=/site/wwwcfm/depute/cvlist54.cfm";

    if !index_cache.exists() {
        let client = crawl::client::ScrapingClient::new();
        let html = client.get(url).await?.text().await?;
        fs::write(&index_cache, &html).await?;
    }

    let content = std::fs::read_to_string(&index_cache)?;
    let document = Html::parse_document(&content);
    let sessions = extract_sessions(&document);

    write_parquet(&parquet_path, &sessions)?;
    println!(
        "Written {} sessions to {}.",
        sessions.len(),
        parquet_path.display()
    );
    Ok(())
}

fn write_parquet(path: &Path, rows: &[ScrapedSession]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("start_date", DataType::Utf8, false),
        Field::new("end_date", DataType::Utf8, false),
    ]));

    macro_rules! col {
        ($f:expr) => {
            Arc::new(StringArray::from(rows.iter().map($f).collect::<Vec<_>>())) as ArrayRef
        };
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            col!(|r| r.session_id.clone()),
            col!(|r| r.start_date.clone()),
            col!(|r| r.end_date.clone()),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn extract_sessions(document: &Html) -> Vec<ScrapedSession> {
    let mut sessions = Vec::new();

    for element in document.select(&SEL_SESSION) {
        let href = match element.value().attr("href") {
            Some(h) => h,
            None => continue,
        };
        let session_id = match href
            .split("legis=")
            .nth(1)
            .and_then(|s| s.split('&').next())
        {
            Some(id) => id.to_string(),
            None => continue,
        };
        let text = element.text().collect::<String>();
        let date_str = text
            .split('(')
            .nth(1)
            .and_then(|s| s.split(')').next())
            .unwrap_or("");

        let parts: Vec<&str> = date_str.split('-').collect();
        if parts.len() != 2 {
            continue;
        }

        sessions.push(ScrapedSession {
            session_id,
            start_date: parts[0].trim().to_string(),
            end_date: parts[1].trim().to_string(),
        });
    }

    sessions
}
