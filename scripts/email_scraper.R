# Load packages
library(mRpostman)
library(mime)
library(stringr)
library(jsonlite)
library(lubridate)

# Connect to Yahoo IMAP
con <- tryCatch({
  configure_imap(
    url = "imaps://imap.mail.yahoo.com/",
    username = Sys.getenv("YAHOO_USER"),
    password = Sys.getenv("YAHOO_PASS"),
    verbose = FALSE
  )
}, error = function(e) {
  message("Error connecting to IMAP: ", e$message)
  NULL
})

if (is.null(con)) stop("Could not connect to Yahoo IMAP.")

con$select_folder(name = "Inbox")

# Search for latest advisory email
emails <- con$search_string(expr = "ECAlertMe", where = "SUBJECT")

# Default to empty if none found
issued_regions <- character(0)

if (length(emails) > 0) {
  latest_email <- tail(emails, 1)
  
  raw_mime <- con$fetch_text(msg_id = latest_email)
  plain_text <- str_extract(raw_mime, "(?s)(?<=Content-Type: text/plain; charset=\"us-ascii\").*?(?=--_)")
  plain_text <- gsub("=\\r\\n|=\\n", "", plain_text)
  plain_text <- gsub("\\r\\n", "\n", plain_text)
  
  locations <- str_match_all(
    plain_text,
    "(?<=statement (?:issued|ended) for:\\s{0,10})([\\s\\S]+?)(?:\\n{2}|Current details|The above alert)"
  )[[1]][, 1]
  
  locations <- str_trim(unlist(strsplit(locations, "\n")))
  locations <- locations[locations != ""]
  location_names <- gsub("\\s*\\(.*\\)", "", locations)
  
  status <- str_extract(plain_text, "(?i)statement (issued|ended)")
  if (tolower(status) == "issued") {
    issued_regions <- location_names
  }
}

# Define all target regions
regions <- c(
  "Sunshine Coast - Gibsons to Earls Cove",
  "Sunshine Coast - Saltery Bay to Powell River",
  "Whistler",
  "Metro Vancouver - NW",
  "Metro Vancouver - NE",
  "Howe Sound",
  "Metro Vancouver - SE",
  "Metro Vancouver - SW",
  "Central Fraser Valley",
  "North Harrison Lake",
  "Eastern Fraser Valley",
  "Fraser Canyon - north including Lillooet",
  "Nicola",
  "Fraser Canyon - south including Lytton"
)

# Build output list
output <- lapply(regions, function(region) {
  list(
    Region = region,
    ActiveAlert = region %in% issued_regions
  )
})

# Add timestamp
final_json <- list(
  lastChecked = format(with_tz(Sys.time(), tzone = "America/Vancouver"), "%Y-%m-%d %H:%M:%S"),
  Advisories = output
)

# Write JSON
write_json(final_json, path = "AQAdvisories.json", pretty = TRUE, auto_unbox = TRUE)
