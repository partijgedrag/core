use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use crawl::paths::{cache_dir, data_dir};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{arrow::ArrowWriter, arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex as TokioMutex;

struct RateLimiter {
    interval_ms: u64,
    min_next_call: TokioMutex<Instant>,
}

impl RateLimiter {
    fn new(requests_per_second: f64) -> Self {
        let interval_ms = (1_000.0 / requests_per_second * 1.15) as u64;
        Self {
            interval_ms,
            min_next_call: TokioMutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut guard = self.min_next_call.lock().await;
        let now = Instant::now();
        if now < *guard {
            tokio::time::sleep(*guard - now).await;
        }
        *guard = Instant::now() + Duration::from_millis(self.interval_ms);
    }

    async fn penalize(&self, delay_ms: u64) {
        let mut guard = self.min_next_call.lock().await;
        *guard = Instant::now() + Duration::from_millis(delay_ms);
    }
}

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 2_000;
const MAX_BACKOFF_MS: u64 = 60_000;
const SAVE_EVERY: usize = 5;

const MODEL_CONTENT: &str = "mistral-large-latest";
const MODEL_ARGUMENTS: &str = "mistral-large-latest";

struct CachedContent {
    summary_hash: String,
    summary: String,
    model: String,
    dossier_id: String,
    source: String,
    created_at: String,
}

struct CachedArguments {
    summary_hash: String,
    arguments: String, // JSON string
    model: String,
    dossier_id: String,
    source: String,
    created_at: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    role: String,
    content: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Choice {
    message: Message,
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiResponse {
    choices: Vec<Choice>,
}

/// The structured JSON we ask the model to fill in.
/// The LLM produces plain-text fields; we assemble the JSON ourselves.
#[derive(Serialize, Deserialize, Debug, Default)]
struct ArgumentsJson {
    title: String,
    description: String,
    arguments_pro: Vec<PartyArgument>,
    arguments_contra: Vec<PartyArgument>,
    arguments_neutral: Vec<PartyArgument>,
    debate_summary: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct PartyArgument {
    parties: String,
    argument: String,
}

fn system_prompt_content() -> &'static str {
    "Je bent een objectieve samenvatter van parlementaire documenten."
}

fn user_prompt_content(content: &str) -> String {
    format!(
        "Je krijgt de volledige tekst van een aangenomen parlementaire tekst. \
Vat de tekst samen voor een gewone kiezer zonder juridische kennis.\n\
- Schrijf in het Nederlands.\n\
- Gebruik maximaal 4 zinnen, hoe korter hoe beter.\n\
- Benadruk het hoofdonderwerp en de concrete gevolgen.\n\
- Houd het objectief — geen politieke interpretatie, alleen feiten.\n\
- Geen extra uitleg, geen opsommingen, enkel de samenvatting.\n\
- Geen voorzetsel zoals 'Samenvatting:' of 'Samenvatting van de tekst:'.\n\n\
Documentinhoud:\n{content}"
    )
}

fn system_prompt_arguments() -> &'static str {
    "Je bent een politiek analist van het Belgische parlement. \
Je antwoordt ALTIJD uitsluitend met geldig JSON. \
Geen extra tekst, geen uitleg, geen markdown code-blokken — enkel de JSON."
}

fn user_prompt_arguments(content: &str) -> String {
    format!(
        "Je krijgt de volledige tekst van een parlementair verslag. \
Analyseer het dossier en geef je antwoord als JSON met EXACT deze structuur \
(geen extra velden, geen commentaar, geen markdown):\n\n\
{{\n\
  \"title\": \"beknopte titel van het dossier (max 15 woorden)\",\n\
  \"description\": \"beknopte beschrijving van wat het dossier inhoudt (1-2 zinnen)\",\n\
  \"arguments_pro\": [\n\
    {{ \"parties\": \"naam van partij/partijen\", \"argument\": \"het argument (1-2 zinnen, geen markdown)\" }}\n\
  ],\n\
  \"arguments_contra\": [\n\
    {{ \"parties\": \"naam van partij/partijen\", \"argument\": \"het argument (1-2 zinnen, geen markdown)\" }}\n\
  ],\n\
  \"arguments_neutral\": [\n\
    {{ \"parties\": \"naam van partij/actor\", \"argument\": \"de neutrale opmerking (1-2 zinnen, geen markdown)\" }}\n\
  ],\n\
  \"debate_summary\": \"alinea die de grote lijnen van het debat samenvat\"\n\
}}\n\n\
Regels:\n\
- Schrijf in het Nederlands.\n\
- arguments_pro en arguments_contra: max 5 items elk, kies de meest relevante.\n\
- arguments_neutral mag een lege array [] zijn als er geen neutrale standpunten zijn.\n\
- Geen **vetgedrukt** in de argumenten of parties-velden.\n\
- Enkel de JSON teruggeven, niets anders.\n\n\
Documentinhoud:\n{content}"
    )
}

/// Parse the model's JSON response into `ArgumentsJson`.
/// Strips optional ```json … ``` fences that some models add despite instructions.
fn parse_arguments_response(raw: &str) -> ArgumentsJson {
    // Strip markdown code fences if present.
    let stripped = raw.trim();
    let json_str = if stripped.starts_with("```") {
        // Remove first line (``` or ```json) and last ``` line.
        let inner: Vec<&str> = stripped.lines().collect();
        let start = 1;
        let end = inner
            .iter()
            .rposition(|l| l.trim() == "```")
            .unwrap_or(inner.len());
        inner[start..end].join("\n")
    } else {
        stripped.to_string()
    };

    serde_json::from_str(&json_str).unwrap_or_else(|e| {
        eprintln!("[summarizer] WARNING: failed to parse arguments JSON: {e}\nRaw:\n{raw}");
        ArgumentsJson::default()
    })
}

fn load_existing_content(path: &Path) -> HashMap<String, CachedContent> {
    let mut map = HashMap::new();
    if !path.exists() {
        return map;
    }
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    for batch in reader {
        let batch = batch.unwrap();
        let hash_col = col_str(&batch, "summary_hash");
        let summary_col = col_str(&batch, "summary");
        let model_col = col_str(&batch, "model");
        let dossier_id_col = col_str(&batch, "dossier_id");
        let source_col = col_str(&batch, "source");
        let created_at_col = col_str(&batch, "created_at");
        for i in 0..batch.num_rows() {
            map.insert(
                hash_col.value(i).to_string(),
                CachedContent {
                    summary_hash: hash_col.value(i).to_string(),
                    summary: summary_col.value(i).to_string(),
                    model: model_col.value(i).to_string(),
                    dossier_id: dossier_id_col.value(i).to_string(),
                    source: source_col.value(i).to_string(),
                    created_at: created_at_col.value(i).to_string(),
                },
            );
        }
    }
    map
}

fn load_existing_arguments(path: &Path) -> HashMap<String, CachedArguments> {
    let mut map = HashMap::new();
    if !path.exists() {
        return map;
    }
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    for batch in reader {
        let batch = batch.unwrap();
        let hash_col = col_str(&batch, "summary_hash");
        let args_col = col_str(&batch, "arguments");
        let model_col = col_str(&batch, "model");
        let dossier_id_col = col_str(&batch, "dossier_id");
        let source_col = col_str(&batch, "source");
        let created_at_col = col_str(&batch, "created_at");
        for i in 0..batch.num_rows() {
            map.insert(
                hash_col.value(i).to_string(),
                CachedArguments {
                    summary_hash: hash_col.value(i).to_string(),
                    arguments: args_col.value(i).to_string(),
                    model: model_col.value(i).to_string(),
                    dossier_id: dossier_id_col.value(i).to_string(),
                    source: source_col.value(i).to_string(),
                    created_at: created_at_col.value(i).to_string(),
                },
            );
        }
    }
    map
}

fn save_content(
    path: &Path,
    cache: &HashMap<String, CachedContent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut hashes, mut summaries, mut models, mut dossier_ids, mut sources, mut created_ats) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for row in cache.values() {
        hashes.push(row.summary_hash.clone());
        summaries.push(row.summary.clone());
        models.push(row.model.clone());
        dossier_ids.push(row.dossier_id.clone());
        sources.push(row.source.clone());
        created_ats.push(row.created_at.clone());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("summary_hash", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(hashes)),
            Arc::new(StringArray::from(summaries)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(dossier_ids)),
            Arc::new(StringArray::from(sources)),
            Arc::new(StringArray::from(created_ats)),
        ],
    )?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn save_arguments(
    path: &Path,
    cache: &HashMap<String, CachedArguments>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut hashes, mut arguments, mut models, mut dossier_ids, mut sources, mut created_ats) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for row in cache.values() {
        hashes.push(row.summary_hash.clone());
        arguments.push(row.arguments.clone());
        models.push(row.model.clone());
        dossier_ids.push(row.dossier_id.clone());
        sources.push(row.source.clone());
        created_ats.push(row.created_at.clone());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("summary_hash", DataType::Utf8, false),
        Field::new("arguments", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("dossier_id", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(hashes)),
            Arc::new(StringArray::from(arguments)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(dossier_ids)),
            Arc::new(StringArray::from(sources)),
            Arc::new(StringArray::from(created_ats)),
        ],
    )?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn col_str<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("Missing column: {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("Column {name} is not a StringArray"))
}

async fn mistral_complete(
    client: &Client,
    api_key: &str,
    system: &str,
    user: &str,
    model: &str,
    call_count: &mut u32,
    rate_limiter: &RateLimiter,
) -> Option<String> {
    let payload = json!({
        "model": model,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user",   "content": user }
        ]
    });

    let mut attempt = 0u32;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        attempt += 1;
        rate_limiter.acquire().await;

        let response = client
            .post("https://api.mistral.ai/v1/chat/completions")
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Bearer {api_key}"))
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let json_resp: ApiResponse = resp.json().await.unwrap();
                *call_count += 1;
                return Some(json_resp.choices[0].message.content.trim().to_string());
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                let status = resp.status();
                let retry_after_ms = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|secs| secs * 1_000 + 500)
                    .unwrap_or(backoff_ms);
                let body = resp.text().await.unwrap_or_default();
                if attempt >= MAX_RETRIES {
                    eprintln!("Mistral retry failed after {attempt} attempts ({status}): {body}");
                    return None;
                }
                eprintln!(
                    "Mistral {status} (attempt {attempt}/{MAX_RETRIES}), retrying in {retry_after_ms}ms… | {body}"
                );
                rate_limiter.penalize(retry_after_ms).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
            Ok(resp) => {
                eprintln!(
                    "Mistral failed {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
                return None;
            }
            Err(err) => {
                if attempt >= MAX_RETRIES {
                    eprintln!("Network error after {attempt} attempts: {err}");
                    return None;
                }
                eprintln!(
                    "Network error (attempt {attempt}/{MAX_RETRIES}): {err}, retrying in {backoff_ms}ms…"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
        }
    }
}

fn hash_text(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Collect all dossier IDs from `summarizers/cache/sessions/56/dossiers/`.
fn discover_dossier_ids(base: &Path) -> Vec<String> {
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(base) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Some(name) = entry.file_name().to_str() {
                    ids.push(name.to_string());
                }
            }
        }
    }
    ids.sort();
    ids
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");

    // Optional: pass a single dossier ID as a CLI argument for testing.
    let single_dossier: Option<String> = Some(String::from("1405")); //std::env::args().nth(1);

    let client = Client::new();

    let dossiers_base = cache_dir().join("summaries/dossiers");
    let content_out = data_dir().join("summaries/dossier_content.parquet");
    let arguments_out = data_dir().join("summaries/dossier_arguments.parquet");

    // Shared rate limiter for mistral-large-latest (1 req/s with margin).
    let rate_limiter = Arc::new(RateLimiter::new(1.0));

    let dossier_ids: Vec<String> = if let Some(ref id) = single_dossier {
        println!("[summarizer] single-dossier mode: {id}");
        vec![id.clone()]
    } else {
        discover_dossier_ids(&dossiers_base)
    };

    println!("[summarizer] found {} dossiers", dossier_ids.len());

    // Load caches.
    let mut content_cache = load_existing_content(&content_out);
    let mut arguments_cache = load_existing_arguments(&arguments_out);
    println!(
        "[summarizer] loaded {} cached content summaries, {} cached argument analyses",
        content_cache.len(),
        arguments_cache.len()
    );

    let mut total_calls = 0u32;
    let mut new_content: usize = 0;
    let mut new_arguments: usize = 0;

    let pb = ProgressBar::new(dossier_ids.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[summarizer] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        )
        .unwrap()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message(format!("api_calls={total_calls}"));

    for dossier_id in &dossier_ids {
        let dossier_dir = dossiers_base.join(dossier_id);

        // ── dossier_content.parquet — from adopted_text.md ──────────────────
        let adopted_path = dossier_dir.join("adopted_text.md");
        if adopted_path.exists() {
            let content = std::fs::read_to_string(&adopted_path).unwrap_or_default();
            if !content.trim().is_empty() {
                let hash = hash_text(&content);
                if !content_cache.contains_key(&hash) {
                    pb.set_message(format!(
                        "api_calls={total_calls} — summarizing content for dossier {dossier_id}"
                    ));
                    let user = user_prompt_content(&content);
                    if let Some(summary) = mistral_complete(
                        &client,
                        &mistral_api_key,
                        system_prompt_content(),
                        &user,
                        MODEL_CONTENT,
                        &mut total_calls,
                        &rate_limiter,
                    )
                    .await
                    {
                        let source = format!("adopted_text.md ({})", adopted_path.display());
                        content_cache.insert(
                            hash.clone(),
                            CachedContent {
                                summary_hash: hash,
                                summary,
                                model: MODEL_CONTENT.to_string(),
                                dossier_id: dossier_id.clone(),
                                source,
                                created_at: now_rfc3339(),
                            },
                        );
                        new_content += 1;

                        if new_content % SAVE_EVERY == 0 {
                            if let Err(e) = save_content(&content_out, &content_cache) {
                                eprintln!(
                                    "[summarizer] WARNING: content checkpoint save failed: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }

        // ── dossier_arguments.parquet — from report.md ───────────────────────
        let report_path = dossier_dir.join("report.md");
        if report_path.exists() {
            let content = std::fs::read_to_string(&report_path).unwrap_or_default();
            let trimmed_content = content.trim();
            if trimmed_content.len() < 500 {
                eprintln!(
                    "[summarizer] WARNING: skipping dossier {dossier_id} report.md — content too short ({} chars < 500)",
                    trimmed_content.len()
                );
            } else if !trimmed_content.is_empty() {
                let hash = hash_text(&content);
                if !arguments_cache.contains_key(&hash) {
                    pb.set_message(format!(
                        "api_calls={total_calls} — analysing arguments for dossier {dossier_id}"
                    ));
                    let user = user_prompt_arguments(&content);
                    if let Some(raw_response) = mistral_complete(
                        &client,
                        &mistral_api_key,
                        system_prompt_arguments(),
                        &user,
                        MODEL_ARGUMENTS,
                        &mut total_calls,
                        &rate_limiter,
                    )
                    .await
                    {
                        let parsed = parse_arguments_response(&raw_response);
                        let arguments_json =
                            serde_json::to_string(&parsed).unwrap_or_else(|_| "{}".to_string());
                        let source = format!("report.md ({})", report_path.display());
                        arguments_cache.insert(
                            hash.clone(),
                            CachedArguments {
                                summary_hash: hash,
                                arguments: arguments_json,
                                model: MODEL_ARGUMENTS.to_string(),
                                dossier_id: dossier_id.clone(),
                                source,
                                created_at: now_rfc3339(),
                            },
                        );
                        new_arguments += 1;

                        if new_arguments % SAVE_EVERY == 0 {
                            if let Err(e) = save_arguments(&arguments_out, &arguments_cache) {
                                eprintln!(
                                    "[summarizer] WARNING: arguments checkpoint save failed: {e}"
                                );
                            }
                        }
                    }
                }
            }
        }

        pb.set_message(format!("api_calls={total_calls}"));
        pb.inc(1);
    }

    pb.finish_with_message(format!("api_calls={total_calls} done"));

    // Final saves.
    save_content(&content_out, &content_cache).expect("Failed to write dossier_content.parquet");
    save_arguments(&arguments_out, &arguments_cache)
        .expect("Failed to write dossier_arguments.parquet");

    println!(
        "[summarizer] finished: {} new content summaries, {} new argument analyses, {} total API calls",
        new_content, new_arguments, total_calls
    );
}
