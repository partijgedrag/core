use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use indicatif::{ProgressBar, ProgressStyle};
use parquet::{arrow::ArrowWriter, arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex as TokioMutex;
use crawl::paths::{cache_dir, data_dir};

struct RateLimiter {
    interval_ms: u64,
    /// Earliest instant at which the next request is allowed.
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

    /// Wait until the rate limit allows a call, then advance the window.
    async fn acquire(&self) {
        let mut guard = self.min_next_call.lock().await;
        let now = Instant::now();
        if now < *guard {
            tokio::time::sleep(*guard - now).await;
        }
        // Next call is not allowed until interval_ms from now.
        *guard = Instant::now() + Duration::from_millis(self.interval_ms);
    }

    /// After a 429, push min_next_call forward by `delay_ms`.
    /// This throttles ALL subsequent rows automatically — not just the
    /// current retry — until the server-side bucket has had time to refill.
    async fn penalize(&self, delay_ms: u64) {
        let mut guard = self.min_next_call.lock().await;
        *guard = Instant::now() + Duration::from_millis(delay_ms);
    }
}

struct ExistingSummary {
    original: String,
    summary: String,
    model: String,
    meeting_id: Option<String>,
    created_at: Option<String>,
}

struct SummarizationTask {
    task_type: SummarizationTaskType,
    model_name: String,
    prompt: String,
    column_name: String,
    source_file: PathBuf,
    output_file: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum SummarizationTaskType {
    PlenaryQuestionTopics,
    PlenaryQuestionDiscussion,
    CommissionQuestionTopics,
    CommissionQuestionDiscussion,
}

impl Display for SummarizationTaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SummarizationTaskType::PlenaryQuestionTopics => write!(f, "PLENARY_QUESTION_TOPICS"),
            SummarizationTaskType::PlenaryQuestionDiscussion => {
                write!(f, "PLENARY_QUESTION_DISCUSSION")
            }
            SummarizationTaskType::CommissionQuestionTopics => {
                write!(f, "COMMISSION_QUESTION_TOPICS")
            }
            SummarizationTaskType::CommissionQuestionDiscussion => {
                write!(f, "COMMISSION_QUESTION_DISCUSSION")
            }
        }
    }
}

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 2_000;
const MAX_BACKOFF_MS: u64 = 60_000;
const SAVE_EVERY: usize = 5;

#[tokio::main]
async fn main() {
    // Load MISTRAL API KEY
    dotenvy::dotenv().ok();

    let mistral_api_key = std::env::var("MISTRAL_API_TOKEN").expect("Missing MISTRAL_API_TOKEN");
    let client = Client::new();

    // Folders
    let summaries = data_dir().join("summaries");

    // One rate limiter per model, shared across all tasks using that model.
    // mistral-medium-2508: 0.38 req/s → ~3 000 ms interval (with margin)
    // mistral-large-latest: 1.00 req/s → ~1 150 ms interval (with margin)
    let mut rate_limiters: HashMap<String, Arc<RateLimiter>> = HashMap::new();
    rate_limiters.insert(
        "mistral-medium-2508".into(),
        Arc::new(RateLimiter::new(0.38)),
    );
    rate_limiters.insert(
        "mistral-large-latest".into(),
        Arc::new(RateLimiter::new(1.0)),
    );
    let rate_limiters = Arc::new(rate_limiters);

    let tasks: Vec<SummarizationTask> = vec![
        SummarizationTask {
            task_type: SummarizationTaskType::PlenaryQuestionTopics,
            model_name: "mistral-large-latest".into(),
            prompt: "The assistant will receive a comma-separated list of topics and generate a \
                single, concise topic (no more than 20 words) that encompasses all the given topics. \
                - The result must match the style of the input topics. \
                - The result must be in Dutch. \
                - Do not add explanations, clarifications, or extra words such as 'including' or 'such as'. \
                - The output should fit naturally within the provided list. \
                - Only return the summarized topic without any additional text.".into(),
            column_name: "topics_nl".into(),
            source_file: data_dir().join("sessions/56/plenary/questions.parquet"),
            output_file: summaries.join("plenary_question_topics.parquet"),
        },
        SummarizationTask {
            task_type: SummarizationTaskType::PlenaryQuestionDiscussion,
            model_name: "mistral-medium-2508".into(),
            prompt: "Je krijgt de volledige discussie (vraag en antwoord) als ruwe tekst. Vat de \
                discussie samen in maximaal 4 zinnen, hoe korter hoe beter. Hou de informatiedensiteit \
                heel hoog, geen onnodige woorden. \
                - Schrijf in het Nederlands. \
                - Benadruk het hoofdonderwerp en de belangrijkste standpunten/antwoorden. \
                – Formuleer waarderende, kritische of beschuldigende uitspraken expliciet als meningen, \
                kritiek of beweringen van de betrokken spreker (bv. \"volgens X\", \"X stelt dat\", \
                \"X bekritiseert dat\"). \
                – Presenteer geen normatieve uitspraken als vaststaande feiten. \
                - Geen extra uitleg, geen opsommingen, enkel de samenvatting. \
                - Gebruik geen em-dashes (—) of andere leestekens als lijstmarkering. \
                - Schrijf in gewone lopende zinnen. Gebruik geen streepjes, bullets of opsommingstekens.
                - De samenvatting moet niet voorafgegaan worden door woorden zoals \"Samenvatting:\" \
                of \"Samenvatting van de discussie:\". Alleen de inhoud moet getoond worden.".into(),
            column_name: "discussion".into(),
            source_file: data_dir().join("sessions/56/plenary/questions.parquet"),
            output_file: summaries.join("plenary_question_discussions.parquet"),
        },
        SummarizationTask {
            task_type: SummarizationTaskType::CommissionQuestionTopics,
            model_name: "mistral-large-latest".into(),
            prompt: "The assistant will receive a comma-separated list of topics and generate a \
                single, concise topic (no more than 20 words) that encompasses all the given topics. \
                - The result must match the style of the input topics. \
                - The result must be in Dutch. \
                - Do not add explanations, clarifications, or extra words such as 'including' or 'such as'. \
                - The output should fit naturally within the provided list. \
                - Only return the summarized topic without any additional text.".into(),
            column_name: "topics_nl".into(),
            source_file: data_dir().join("sessions/56/commission/questions.parquet"),
            output_file: summaries.join("commission_question_topics.parquet"),
        },
        SummarizationTask {
            task_type: SummarizationTaskType::CommissionQuestionDiscussion,
            model_name: "mistral-medium-2508".into(),
            prompt: "Je krijgt de volledige discussie (vraag en antwoord) als ruwe tekst. Vat de \
                discussie samen in maximaal 4 zinnen, hoe korter hoe beter. Hou de informatiedensiteit \
                heel hoog, geen onnodige woorden. \
                - Schrijf in het Nederlands. \
                - Benadruk het hoofdonderwerp en de belangrijkste standpunten/antwoorden. \
                – Formuleer waarderende, kritische of beschuldigende uitspraken expliciet als meningen, \
                kritiek of beweringen van de betrokken spreker (bv. \"volgens X\", \"X stelt dat\", \
                \"X bekritiseert dat\"). \
                – Presenteer geen normatieve uitspraken als vaststaande feiten. \
                - Geen extra uitleg, geen opsommingen, enkel de samenvatting.".into(),
            column_name: "discussion".into(),
            source_file: data_dir().join("sessions/56/commission/questions.parquet"),
            output_file: summaries.join("commission_question_discussions.parquet"),
        },
    ];

    // Run tasks
    let mut total_calls = 0u32;
    for task in tasks {
        let limiter = Arc::clone(
            rate_limiters
                .get(&task.model_name)
                .unwrap_or_else(|| panic!("No rate limiter for model {}", task.model_name)),
        );
        total_calls += run_summarization_task(task, &client, &mistral_api_key, limiter).await;
    }

    println!(
        "Summarized with a total of {} Mistral API calls",
        total_calls
    );
}

async fn run_summarization_task(
    task: SummarizationTask,
    client: &Client,
    api_key: &str,
    rate_limiter: Arc<RateLimiter>,
) -> u32 {
    let mut existing = load_existing_summaries(&task.output_file);
    println!(
        "[summarizer] loaded {} cached summaries from {:?}",
        existing.len(),
        task.output_file
    );

    struct Row {
        meeting_id: String,
        raw_input: String,
    }

    let mut rows: Vec<Row> = Vec::new();
    let source_file = File::open(&task.source_file).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(source_file)
        .unwrap()
        .build()
        .unwrap();

    for batch_result in reader {
        let batch = batch_result.expect("Failed to read batch");
        let source_col = batch
            .column_by_name(task.column_name.as_str())
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let meeting_id_col = batch
            .column_by_name("meeting_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..source_col.len() {
            rows.push(Row {
                meeting_id: meeting_id_col.value(i).to_string(),
                raw_input: source_col.value(i).to_string(),
            });
        }
    }

    // Sort by meeting id
    rows.sort_by(|a, b| {
        let ai = a.meeting_id.parse::<u64>().unwrap_or(0);
        let bi = b.meeting_id.parse::<u64>().unwrap_or(0);
        if ai != 0 || bi != 0 {
            bi.cmp(&ai)
        } else {
            b.meeting_id.cmp(&a.meeting_id)
        }
    });

    let mut mistral_calls = 0u32;
    let mut new_since_save: usize = 0;

    let pb = ProgressBar::new(rows.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[summarizer] [{elapsed_precise}] {spinner:.blue} {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {msg}",
        ).unwrap().tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message(format!("reqs={} {}", mistral_calls, task.task_type));

    for row in &rows {
        let input_hash = hash_text(&row.raw_input);
        let should_summarize =
            needs_summarization(&task.task_type, &row.raw_input, &input_hash, &existing);

        if should_summarize {
            pb.set_message(format!(
                "reqs={} {} — calling API…",
                mistral_calls, task.task_type
            ));
            if let Some(summary) = mistral_complete(
                client,
                api_key,
                &row.raw_input,
                &task.model_name,
                &task.prompt,
                &mut mistral_calls,
                &rate_limiter,
            )
            .await
            {
                existing.insert(
                    input_hash,
                    ExistingSummary {
                        original: row.raw_input.clone(),
                        summary,
                        model: task.model_name.clone(),
                        meeting_id: Some(row.meeting_id.clone()),
                        created_at: Some(chrono::Utc::now().to_rfc3339()),
                    },
                );
                new_since_save += 1;

                if new_since_save >= SAVE_EVERY {
                    std::fs::create_dir_all(task.output_file.parent().unwrap()).unwrap();
                    if let Err(e) = rewrite_summaries_file(&task.output_file, &existing) {
                        eprintln!("[{}] WARNING: periodic save failed: {e}", task.task_type);
                    } else {
                        pb.set_message(format!(
                            "reqs={} {} — saved checkpoint",
                            mistral_calls, task.task_type
                        ));
                    }
                    new_since_save = 0;
                }
            }
        } else if let Some(e) = existing.get_mut(&input_hash) {
            e.meeting_id = Some(row.meeting_id.clone());
        }

        pb.inc(1);
    }

    pb.finish_with_message(format!("reqs={} {} done", mistral_calls, task.task_type));

    // Final save (always, even if new_since_save == 0 — updates meeting_ids).
    std::fs::create_dir_all(task.output_file.parent().unwrap()).unwrap();
    rewrite_summaries_file(&task.output_file, &existing).expect("Failed to write summaries");

    println!(
        "[summarizer] finished task: {} with {} new Mistral calls, {} total rows written",
        task.task_type,
        mistral_calls,
        existing.len(),
    );
    mistral_calls
}

/// Whether a row needs a fresh API call.
fn needs_summarization(
    task_type: &SummarizationTaskType,
    raw_input: &str,
    input_hash: &str,
    existing: &HashMap<String, ExistingSummary>,
) -> bool {
    if existing.contains_key(input_hash) {
        return false;
    }
    match task_type {
        // only summary if multiple topics
        SummarizationTaskType::PlenaryQuestionTopics
        | SummarizationTaskType::CommissionQuestionTopics => raw_input.contains(';'),
        SummarizationTaskType::PlenaryQuestionDiscussion
        | SummarizationTaskType::CommissionQuestionDiscussion => {
            let t = raw_input.trim();
            t != "[]" && !t.is_empty()
        }
    }
}

fn load_existing_summaries(path: &PathBuf) -> HashMap<String, ExistingSummary> {
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

        let input_hash_col = col_as_strings(&batch, "input_hash");
        let original_col = col_as_strings(&batch, "original");
        let summary_col = col_as_strings(&batch, "summary");
        let model_col = col_as_strings(&batch, "model");
        let meeting_id_col = batch
            .column_by_name("meeting_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let created_at_col = batch
            .column_by_name("created_at")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for i in 0..batch.num_rows() {
            map.insert(
                input_hash_col.value(i).to_string(),
                ExistingSummary {
                    original: original_col.value(i).to_string(),
                    summary: summary_col.value(i).to_string(),
                    model: model_col.value(i).to_string(),
                    meeting_id: meeting_id_col.map(|c| c.value(i).to_string()),
                    created_at: created_at_col.map(|c| c.value(i).to_string()),
                },
            );
        }
    }

    map
}

fn col_as_strings<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("Missing column: {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("Column {name} is not a StringArray"))
}

fn rewrite_summaries_file(
    path: &PathBuf,
    existing: &HashMap<String, ExistingSummary>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut hashes, mut originals, mut summaries, mut models, mut meeting_ids, mut created_at) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);

    for (hash, row) in existing {
        let created = row
            .created_at
            .clone()
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        hashes.push(hash.clone());
        originals.push(row.original.clone());
        summaries.push(row.summary.clone());
        models.push(row.model.clone());
        meeting_ids.push(row.meeting_id.clone().unwrap_or_default());
        created_at.push(created);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("input_hash", DataType::Utf8, false),
        Field::new("original", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("meeting_id", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(hashes)),
            Arc::new(StringArray::from(originals)),
            Arc::new(StringArray::from(summaries)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(meeting_ids)),
            Arc::new(StringArray::from(created_at)),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
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

async fn mistral_complete(
    client: &Client,
    api_key: &str,
    content: &str,
    model: &str,
    prompt: &str,
    mistral_calls: &mut u32,
    rate_limiter: &RateLimiter,
) -> Option<String> {
    let payload = json!({
        "model": model,
        "messages": [
            { "role": "system", "content": prompt },
            { "role": "user",   "content": content }
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
            .header(AUTHORIZATION, format!("Bearer {}", api_key))
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                let json_resp: ApiResponse = resp.json().await.unwrap();
                *mistral_calls += 1;
                return Some(strip_markdown(&json_resp.choices[0].message.content));
            }
            Ok(resp) if resp.status().as_u16() == 429 || resp.status().is_server_error() => {
                let status = resp.status();

                // Retry-After (seconds) if present, otherwise exponential backoff.
                // Note: Mistral currently does NOT send Retry-After, so we fall back.
                let retry_after_ms = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|secs| secs * 1_000 + 500)
                    .unwrap_or(backoff_ms);

                let body = resp.text().await.unwrap_or_default();
                if attempt >= MAX_RETRIES {
                    eprintln!(
                        "Mistral retry failed after {attempt} attempts. Last error ({status}): {body}"
                    );
                    return None;
                }
                eprintln!(
                    "Mistral {status} (attempt {attempt}/{MAX_RETRIES}), \
                     next attempt in {retry_after_ms}ms… | {body}"
                );

                // Penalise the shared limiter — all rows queued after this will also
                // wait, preventing a burst of requests into a still-depleted bucket.
                rate_limiter.penalize(retry_after_ms).await;
                // No explicit sleep here — acquire() at the top of the next
                // loop iteration will sleep until min_next_call.

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

fn strip_markdown(input: &str) -> String {
    let mut s = input.replace("**", "");
    s = s.replace("__", "");
    s = s.replace('*', "");
    s = s.replace('_', "");
    s.trim().to_string()
}
