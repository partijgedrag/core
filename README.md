# `⚙️ Belgian Parliamentary Data Scrapers`

Tools to collect structure, and summarize data about the Belgian federal parliament (Chamber of Representatives).

This project provides software that scrapes publicly available parliamentary records published by the Belgian Chamber of Representatives. This is data that citizens have a right to access and that exists in the public interest of democratic transparency. Requests are throttled and cached to avoid putting unnecessary load on the source servers, and only publicly accessible pages are fetched. No authentication bypass, paywall circumvention, or scraping of non-public data. The code is licensed under GPLv3, meaning it stays open: anyone building on it must share their improvements back. While the license doesn't legally restrict commercial use, the spirit of this project is purely civic, educational and non-commercial. If you're using this data or code commercially, we'd appreciate you reaching out, crediting the project, and considering contributing back.

Also see [DISCLAIMER.md](DISCLAIMER.md).

## Structure

```bash
scrapers/              
  commission-meetings/  # commission meeting reports
  commissions/          # chamber commissions
  dossiers/             # dossiers
  lobby/                # lobby members
  members/              # chamber members
  plenary-meetings/     # plenary meeting reports
  remunerations/        # remunerations of members
  sessions/             # chamber sessions
summarizers/            # summarize topics/dossiers/discussions
```

## Scrapers

Every scraper follows the same pipeline:

1. **Fetch:** Download a `HTML` page, or read it from `cache/` if it already exists 
2. **Extract:** Parse the HTML and extract structured data from it
3. **Write:** Serialize the extracted data into `.parquet` files under `data/`

### Data and Cache

The `data` directory contains the generated `.parquet` files.

The `cache` directory contains the stored `.HTML` and `.PDF` files which are stored to avoid calling the website `dekamer.be` unnecessarily. A file is only fetched if it does not already exist in the cache or if it needs updating.

Both these directories can be set through environment variables.

### Running

See the `justfile` for all the available commands.

An `.env` file is expected with these environment variables.

```
SCRAPER_DATA_DIR="./data"
SCRAPER_CACHE_DIR="./cache"
SCRAPER_PROJECT_NAME="yourproject"
SCRAPER_PROJECT_URL="yourproject.example"
SCRAPER_CONTACT_EMAIL="your@email.com"
```

## Summarizers

The summarizers summarizes topics, dossiers and discussions using the Mistral API. This requires a `MISTRAL_API_TOKEN` to be set in the `.env` file.

The summarizers do the following:

- summarize multiple question topics into a single encompassing topic
- summarize question discussions
- summarize dossiers

The dossier summarization flow works as follows:

1. Download the dossier report + adopted text PDFs (no HTML versions exists)
2. Convert the PDF to Markdown using `dossier-pdf-to-markdown`
3. Summarize the Markdown contents

### Running

See the `justfile` for all the available commands.

An `.env` file is expected with these environment variables.

```
MISTRAL_API_TOKEN="123"
```

## Known scraping issues

These issues are known issues in the scraper that should be fixed.

### Commission meetings

- [ ] "Schriftelijk behandelde mondelinge vragen" are not correctly scraped (discussion + respondents are not captured)

## Known data issues

These issues are known issues in the underlying data that have been or will be communicated to De Kamer and might be fixed by them.

**Fixed by De Kamer**
- ~~Dossier 1236, subdocument 006 has a date of 20/03/2023 which should be 20/03/2026~~
- ~~Dossier 1280, subdocument 042 has a date of 13/03/2023 which should be 13/03/2026~~
- ~~Dossier 587, subdocument 003 has a date of 03/01/2024 which should be 03/01/2025~~
- ~~Plenary 19, vote 2: vote counts do not match member counts in vote details~~
- ~~Plenary 60, vote 132: vote counts do not match member counts in vote details~~
- ~~Plenary 117, vote 45: vote counts do not match member counts in vote details~~
- ~~Plenary 117, vote 84: vote counts do not match member counts in vote details~~


**Acknowleged by De Kamer, not to be fixed or not fixed yet**
- Plenary 71, question 1 contains the name 'Steven Coengrachts' instead of 'Steven Coenegrachts' (human-made typo)


**Not yet communicated to De Kamer**
- Plenary 5, question 1 about 'Het uitstellen van de indiening van het begrotingsplan bij de EU' has ID '56000005P' but question 4 about 'Audi Vorst' also has ID '56000005P'.
