use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crawl::paths::{cache_dir, data_dir};
use parquet::arrow::ArrowWriter;
use scraper::{ElementRef, Html, Selector};
use std::error::Error;
use std::fs::{File, read_to_string};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::fs;

static ROW_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("tr").unwrap());
static CELL_SELECTOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("td").unwrap());

#[derive(Debug)]
struct ScrapedLobby {
    name: String,
    contacts: String,
    interests: String,
    url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let lobby_path = data_dir().join("lobby.parquet");
    fs::create_dir_all(lobby_path.parent().unwrap()).await?;

    let source_path = cache_dir().join("lobby/lobbyregister.html");

    if !source_path.exists() {
        // DOWNLOAD AND CONVERT TO PDF TO HTML?
        // Replace with actual source URL
        // let url = "YOUR_URL_HERE";

        // let content = client.get(url).await?.text().await?;

        // fs::create_dir_all(source_path.parent().unwrap()).await?;
        // fs::write(&source_path, &content).await?;
    }

    let content = read_to_string(&source_path)?;
    let document = Html::parse_document(&content);

    let lobby = extract_lobby(document)?;
    write_parquet(&lobby_path, &lobby)?;

    println!("Scraped {} lobby entries.", lobby.len());

    Ok(())
}

fn write_parquet(path: &Path, rows: &[ScrapedLobby]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("contacts", DataType::Utf8, false),
        Field::new("interests", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
    ]));

    macro_rules! col {
        ($f:expr) => {
            Arc::new(StringArray::from(rows.iter().map($f).collect::<Vec<_>>())) as ArrayRef
        };
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            col!(|r| r.name.clone()),
            col!(|r| r.contacts.clone()),
            col!(|r| r.interests.clone()),
            col!(|r| r.url.clone()),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn extract_lobby(document: Html) -> Result<Vec<ScrapedLobby>, Box<dyn Error>> {
    let rows: Vec<_> = document.select(&ROW_SELECTOR).skip(1).collect();
    let total_rows = rows.len();

    let mut lobby = Vec::with_capacity(total_rows);

    for row in rows.into_iter() {
        lobby.push(ScrapedLobby {
            name: extract_cell(&row, 0),
            contacts: extract_contacts(&row, 1),
            interests: extract_cell(&row, 2),
            url: extract_cell(&row, 3),
        });
    }

    Ok(lobby)
}

fn extract_contacts(row: &ElementRef, index: usize) -> String {
    row.select(&CELL_SELECTOR)
        .nth(index)
        .map(|cell| {
            cell.text()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

fn extract_cell(row: &ElementRef, index: usize) -> String {
    row.select(&CELL_SELECTOR)
        .nth(index)
        .map(|cell| cell.text().collect::<Vec<_>>().join(" ").trim().to_string())
        .unwrap_or_default()
}
