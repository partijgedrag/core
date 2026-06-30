use rand::RngExt;
use std::time::Duration;
use tokio::time::sleep;

use reqwest::{Client, Error, Response, header};

pub struct ScrapingClient {
    client: Client,
    user_agent: header::HeaderValue,
}

impl ScrapingClient {
    pub fn new() -> Self {
        dotenvy::dotenv().ok();

        let client = Client::builder()
            .tcp_keepalive(Duration::from_secs(30))
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        let project_name =
            std::env::var("SCRAPER_PROJECT_NAME").expect("Missing SCRAPER_PROJECT_NAME");
        let project_url =
            std::env::var("SCRAPER_PROJECT_URL").expect("Missing SCRAPER_PROJECT_URL");
        let contact_email =
            std::env::var("SCRAPER_CONTACT_EMAIL").expect("Missing SCRAPER_CONTACT_EMAIL");

        const PLACEHOLDER_PROJECT_NAME: &str = "yourproject";
        const PLACEHOLDER_PROJECT_URL: &str = "yourproject.example";
        const PLACEHOLDER_CONTACT_EMAIL: &str = "your@email.com";

        if project_name == PLACEHOLDER_PROJECT_NAME
            || project_url == PLACEHOLDER_PROJECT_URL
            || contact_email == PLACEHOLDER_CONTACT_EMAIL
        {
            panic!(
                "PLACEHOLDER_PROJECT_NAME or SCRAPER_PROJECT_URL or SCRAPER_CONTACT_EMAIL is still set to the placeholder \
                                value from .env.example (got url={:?}, email={:?}). Set real values before scraping.",
                project_url, contact_email
            );
        }

        let user_agent_str = format!("{}/0.1 (+{}; {})", project_name, project_url, contact_email);
        let user_agent = header::HeaderValue::from_str(&user_agent_str)
            .expect("Invalid characters in SCRAPER_PROJECT_URL or SCRAPER_CONTACT_EMAIL");

        ScrapingClient { client, user_agent }
    }

    pub async fn get(&self, url: &str) -> Result<Response, Error> {
        let delay_ms = rand::rng().random_range(1000..4000);
        sleep(Duration::from_millis(delay_ms)).await;
        self.client.get(url).headers(self.headers()).send().await
    }

    fn headers(&self) -> header::HeaderMap {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::USER_AGENT, self.user_agent.clone());
        // headers.insert(header::USER_AGENT, header::HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"));
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"));
        headers.insert(
            header::ACCEPT_LANGUAGE,
            header::HeaderValue::from_static("en-US,en;q=0.9,nl;q=0.8"),
        );
        headers.insert(
            header::CONNECTION,
            header::HeaderValue::from_static("keep-alive"),
        );
        // headers.insert(
        //     header::UPGRADE_INSECURE_REQUESTS,
        //     header::HeaderValue::from_static("1"),
        // );
        // headers.insert(
        //     header::REFERER,
        //     header::HeaderValue::from_static("https://www.dekamer.be"),
        // );

        headers
    }
}
