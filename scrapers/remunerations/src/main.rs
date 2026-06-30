use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use crawl::paths::{cache_dir, data_dir};
use headless_chrome::Browser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::error::Error;
use std::fs::{File, read_to_string};
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::fs;

static SEL_ROW: LazyLock<Selector> = LazyLock::new(|| Selector::parse("tbody tr").unwrap());
static SEL_MANDATE: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("tbody td[aria-colindex='3']").unwrap());
static SEL_INSTITUTE: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("tbody td[aria-colindex='4'] button").unwrap());
static SEL_REMUNERATION: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("tbody td[aria-colindex='6']").unwrap());
static SEL_NO_RESULT: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("tbody tr.k-grid-norecords td").unwrap());

#[derive(Debug)]
struct ScrapedRemuneration {
    first_name: String,
    last_name: String,
    year: u32,
    mandate: String,
    institute: String,
    remuneration_min: String,
    remuneration_max: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();

    let browser = Browser::default()?;
    let tab = browser.new_tab()?;

    let members_path = data_dir().join("sessions/56/members.parquet");
    let remunerations_path = data_dir().join("remunerations.parquet");
    fs::create_dir_all(remunerations_path.parent().unwrap()).await?;

    let mut seen = HashSet::new();
    let mut members = Vec::new();
    let mut all_remunerations = Vec::new();
    let mut web_requests = 0u32;

    // Collect unique members first
    let members_file = File::open(&members_path)?;
    let reader = SerializedFileReader::new(members_file)?;
    let mut iter = reader.get_row_iter(None)?;

    while let Some(row_result) = iter.next() {
        let row = row_result?;
        let first_name = row.get_string(2)?.to_string();
        let last_name = row.get_string(3)?.to_string();
        let full_name = format!("{first_name} {last_name}");

        if seen.insert(full_name) {
            members.push((first_name, last_name));
        }
    }

    let total_steps = (members.len() * 7) as u64;

    let pb = ProgressBar::new(total_steps);
    pb.set_style(
        ProgressStyle::with_template(
            "[remunerations] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )?
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    pb.set_message(web_requests.to_string());

    for (first_name, last_name) in members {
        for year in 2018..=2024 {
            pb.set_message(format!(
                "reqs={} {} {} ({})",
                web_requests, first_name, last_name, year
            ));

            let mut rows =
                extract_remunerations(&tab, &first_name, &last_name, year, &mut web_requests)
                    .await?;

            all_remunerations.append(&mut rows);

            pb.inc(1);
        }
    }

    pb.finish_with_message("done");
    write_parquet(&remunerations_path, &all_remunerations)?;

    println!(
        "Scraped {} remuneration rows using {} web request(s).",
        all_remunerations.len(),
        web_requests
    );
    Ok(())
}

fn write_parquet(path: &Path, rows: &[ScrapedRemuneration]) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("year", DataType::Utf8, false),
        Field::new("mandate", DataType::Utf8, false),
        Field::new("institute", DataType::Utf8, false),
        Field::new("remuneration_min", DataType::Utf8, false),
        Field::new("remuneration_max", DataType::Utf8, false),
    ]));

    macro_rules! col {
        ($f:expr) => {
            Arc::new(StringArray::from(rows.iter().map($f).collect::<Vec<_>>())) as ArrayRef
        };
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            col!(|r| r.first_name.clone()),
            col!(|r| r.last_name.clone()),
            col!(|r| r.year.to_string()),
            col!(|r| r.mandate.clone()),
            col!(|r| r.institute.clone()),
            col!(|r| r.remuneration_min.clone()),
            col!(|r| r.remuneration_max.clone()),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

async fn extract_remunerations(
    tab: &headless_chrome::Tab,
    first_name: &str,
    last_name: &str,
    year: u32,
    web_requests: &mut u32,
) -> Result<Vec<ScrapedRemuneration>, Box<dyn Error>> {
    let cache_path = cache_dir().join(format!(
        "remunerations/{}-{}-{}.html",
        last_name, first_name, year
    ));

    if !cache_path.exists() {
        let url = format!(
            "https://public.regimand.be/?mandatary={} {}&year={}",
            first_name, last_name, year
        );
        tab.navigate_to(&url)?;
        *web_requests += 1;
        tab.wait_for_element("kendo-autocomplete")?;

        let html = tab.get_content()?;
        fs::create_dir_all(cache_path.parent().unwrap()).await?;
        fs::write(&cache_path, &html).await?;
    }

    let content = read_to_string(&cache_path)?;
    let document = Html::parse_document(&content);

    // Early-exit when the page reports no results.
    if document
        .select(&SEL_NO_RESULT)
        .any(|el| el.text().any(|t| t.contains("Geen resultaat gevonden")))
    {
        return Ok(vec![]);
    }

    let mut rows = Vec::new();
    for row in document.select(&SEL_ROW) {
        let mandate = row
            .select(&SEL_MANDATE)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|| "Unknown".to_string());

        let institute = row
            .select(&SEL_INSTITUTE)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" "))
            .unwrap_or_else(|| "Unknown".to_string());

        let (remuneration_min, remuneration_max) = row
            .select(&SEL_REMUNERATION)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" "))
            .and_then(|raw| clean_remuneration(&raw))
            .unwrap_or_else(|| (String::new(), String::new()));

        rows.push(ScrapedRemuneration {
            first_name: first_name.to_string(),
            last_name: last_name.to_string(),
            year,
            mandate,
            institute,
            remuneration_min,
            remuneration_max,
        });
    }

    Ok(rows)
}

fn clean_remuneration(raw: &str) -> Option<(String, String)> {
    if raw.contains("Niet bezoldigd") {
        return Some(("0".to_string(), "0".to_string()));
    }

    let cleaned = raw
        .replace("Afgerond op ", "")
        .replace('\u{00a0}', "") // non-breaking space
        .replace(['€', '&', ' ', ','], "")
        .replace(',', ".");

    if let Some((left, right)) = cleaned.split_once('-') {
        if let (Ok(start), Ok(end)) = (left.trim().parse::<f64>(), right.trim().parse::<f64>()) {
            return Some((start.to_string(), end.to_string()));
        }
    } else if let Ok(value) = cleaned.parse::<f64>() {
        return Some((value.to_string(), value.to_string()));
    }

    None
}
