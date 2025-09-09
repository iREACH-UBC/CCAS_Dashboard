#!/usr/bin/env Rscript

# ──────────────────────────────────────────────────────────────────────────────
# AQ Advisory Email Harvester (Yahoo IMAP) with Umbrella-Region Expansion
# ──────────────────────────────────────────────────────────────────────────────
# - Connects to Yahoo IMAP (INBOX)
# - Finds emails with subject containing "Alert for AQ Alerts"
# - Parses MIME safely to extract text/plain
# - Extracts advisory blocks, expands umbrella regions to specific sub-regions
# - Maintains a duplicate-safe history log and an active-alerts state
# - Writes a compact AQAdvisories.json with booleans per target region
# ──────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(mRpostman)
  library(mime)
  library(stringr)
  library(jsonlite)
  library(lubridate)
  library(dplyr)
})

# ──────────────────────────────────────────────────────────────────────────────
# Config / Files
# ──────────────────────────────────────────────────────────────────────────────
LOG_FILE      <- "processed_emails.txt"
HISTORY_FILE  <- "alert_history.txt"
ACTIVE_FILE   <- "active_alerts.txt"
OUTPUT_JSON   <- "AQAdvisories.json"
TIMEZONE_OUT  <- "America/Vancouver"

# The canonical, specific regions you want booleans for in the output JSON
TARGETS <- data.frame(
  Region = c(
    "Sunshine Coast - Gibsons to Earls Cove, B.C.",
    "Sunshine Coast - Saltery Bay to Powell River, B.C.",
    "Whistler, B.C.",
    "Metro Vancouver - NW, B.C.",
    "Metro Vancouver - NE, B.C.",
    "Howe Sound, B.C.",
    "Metro Vancouver - SE, B.C.",
    "Metro Vancouver - SW, B.C.",
    "Central Fraser Valley, B.C.",
    "North Harrison Lake, B.C.",
    "Eastern Fraser Valley, B.C.",
    "Fraser Canyon - north including Lillooet, B.C.",
    "Nicola, B.C.",
    "Fraser Canyon - south including Lytton, B.C."
  ),
  Code = NA_character_,            # Fill if you have stable codes for each region
  stringsAsFactors = FALSE
)

# Umbrella name → which specific regions to expand to
UMBRELLA_MAP <- list(
  "(?i)^sunshine\\s*coast$" = c(
    "Sunshine Coast - Gibsons to Earls Cove, B.C.",
    "Sunshine Coast - Saltery Bay to Powell River, B.C."
  ),
  "(?i)^metro\\s*vancouver$" = c(
    "Metro Vancouver - NW, B.C.",
    "Metro Vancouver - NE, B.C.",
    "Metro Vancouver - SE, B.C.",
    "Metro Vancouver - SW, B.C."
  ),
  "(?i)^fraser\\s*canyon$" = c(
    "Fraser Canyon - north including Lillooet, B.C.",
    "Fraser Canyon - south including Lytton, B.C."
  )
  # Example to add:
  # "(?i)^fraser\\s*valley$" = c("Central Fraser Valley, B.C.", "Eastern Fraser Valley, B.C.")
)

# For the JSON’s “Advisories” array, keep the Regions in the same order as TARGETS
REGIONS_FOR_JSON <- TARGETS$Region

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

normalize_key <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- stringr::str_replace_all(x, "[\u2010-\u2015]", "-")  # any dash → "-"
  x <- stringr::str_replace_all(x, "[“”]", "\"")
  x <- gsub("\\s+", " ", trimws(tolower(x)))
  x
}

clean_plain_text <- function(text) {
  if (is.null(text) || length(text) == 0 || is.na(text)) return("")
  text <- gsub("\\r\\n|\\r", "\n", text)   # normalize \r\n → \n
  text <- gsub("\n{3,}", "\n\n", text)     # collapse excessive blank lines
  trimws(text)
}

# Recursively extract the first text/plain part from a parsed MIME object
get_text_plain <- function(node) {
  ct <- node$headers$`content-type`
  if (!is.null(ct) && grepl("^text/plain", tolower(ct))) {
    # node$body is raw; convert to character
    return(clean_plain_text(rawToChar(node$body)))
  }
  if (!is.null(node$parts)) {
    for (p in node$parts) {
      got <- get_text_plain(p)
      if (!is.null(got) && nzchar(got)) return(got)
    }
  }
  NULL
}

# Extract advisory info (time/status/regions) from a plain-text email
extract_info <- function(raw_mime, plain_text, email_id) {
  # Attempt to find "Issued at 2025-09-09 10:23 AM PDT" (variants tolerated)
  time_of_issue <- str_match(raw_mime, "Issued at ([0-9\\-]{10}\\s+[0-9:APM ]+\\s+[A-Z]{2,4})")
  if (is.na(time_of_issue[1, 2])) {
    # fallback: use now
    dt <- with_tz(Sys.time(), TIMEZONE_OUT)
  } else {
    datetime_str <- time_of_issue[1, 2]
    dt <- suppressWarnings(parse_date_time(
      datetime_str,
      orders = c("Y-m-d I:M p z", "Y-m-d H:M:S z", "Y-m-d H:M z", "Y-m-d H:M:S")
    ))
    if (is.na(dt)) dt <- Sys.time()
    dt <- with_tz(dt, TIMEZONE_OUT)
  }
  dt_str <- format(dt, "%Y-%m-%d %H:%M:%S")
  
  # Find blocks like: "Air quality statement - issued for:\n<locations...>"
  pattern <- "(?is)air\\s+quality\\s+(warning|statement)\\s*[-–—]\\s*(issued|ended|continued)\\s*for:\\s*([\\s\\S]+?)(?=\\n{2,}|The\\s+above\\s+alert|Current\\s+details|Air\\s+quality\\s+(warning|statement)|\\z)"
  alert_matches <- str_match_all(plain_text, pattern)[[1]]
  if (is.null(alert_matches) || nrow(alert_matches) == 0) {
    message("No alert blocks found for email ID: ", email_id)
    return(NULL)
  }
  
  all_dfs <- list()
  
  for (j in seq_len(nrow(alert_matches))) {
    status <- tolower(alert_matches[j, 2])  # warning/statement (not used)
    action <- tolower(alert_matches[j, 3])  # issued/ended/continued
    locations_block <- alert_matches[j, 4]
    
    # Split by lines; typical lines look like "Metro Vancouver - NE, B.C. (MV-NE)" or sometimes no code
    lines <- str_split(locations_block, "\n")[[1]]
    lines <- trimws(lines)
    lines <- lines[nzchar(lines)]
    
    if (!length(lines)) next
    
    # Extract Region and Code if present
    region_names <- gsub("\\s*\\(.*\\)", "", lines)
    codes <- str_extract(lines, "\\(([^)]+)\\)")
    codes <- gsub("[()]", "", codes)
    
    # Build frame, keep rows where either Region or Code is present
    tmp <- data.frame(
      Region = region_names,
      Code = ifelse(is.na(codes), NA_character_, codes),
      Status = action,
      EmailTimestamp = dt_str,
      EmailID = as.character(email_id),
      stringsAsFactors = FALSE
    )
    tmp <- tmp[!(is.na(tmp$Region) & is.na(tmp$Code)), , drop = FALSE]
    
    if (nrow(tmp)) all_dfs[[length(all_dfs) + 1]] <- tmp
  }
  
  if (!length(all_dfs)) return(NULL)
  do.call(rbind, all_dfs)
}

# Expand umbrella rows into the specific sub-regions you track
expand_umbrella_regions <- function(df) {
  if (is.null(df) || !nrow(df)) return(df)
  
  # Pre-compute for TARGETS
  TARGETS$RegionKey <<- normalize_key(TARGETS$Region)
  
  out_list <- vector("list", nrow(df))
  for (i in seq_len(nrow(df))) {
    row <- df[i, , drop = FALSE]
    expanded <- NULL
    matched_any <- FALSE
    
    for (pat in names(UMBRELLA_MAP)) {
      if (grepl(pat, row$Region, perl = TRUE)) {
        matched_any <- TRUE
        targets <- UMBRELLA_MAP[[pat]]
        
        # Create rows for each specific region
        tgt_rows <- lapply(targets, function(rname) {
          tgt <- TARGETS[TARGETS$Region == rname, , drop = FALSE]
          # Prefer incoming Code; else use TARGETS' Code (if defined), else NA
          code <- dplyr::coalesce(row$Code, if (nrow(tgt)) tgt$Code[1] else NA_character_)
          data.frame(
            Region = rname,
            Code = code,
            Status = row$Status,
            EmailTimestamp = row$EmailTimestamp,
            EmailID = row$EmailID,
            stringsAsFactors = FALSE
          )
        })
        expanded <- rbind(expanded, do.call(rbind, tgt_rows))
      }
    }
    
    # If no umbrella matched, keep the original row
    out_list[[i]] <- if (matched_any && !is.null(expanded)) expanded else
      row[, c("Region","Code","Status","EmailTimestamp","EmailID"), drop = FALSE]
  }
  
  res <- do.call(rbind, out_list)
  
  # Deduplicate (umbrella + already-specific rows)
  res$.__key <- paste0(
    normalize_key(res$Region), "|",
    ifelse(is.na(res$Code), "", res$Code), "|",
    normalize_key(res$Status), "|",
    res$EmailTimestamp
  )
  res <- res[!duplicated(res$.__key), ]
  res$.__key <- NULL
  res
}

# Append new rows to history, skipping duplicates via stable key
append_new_history <- function(temp_df, history_file) {
  if (!file.exists(history_file)) {
    write.table(temp_df, file = history_file, append = FALSE, sep = "\t",
                row.names = FALSE, col.names = TRUE, quote = FALSE)
    return(invisible(TRUE))
  }
  existing <- read.table(history_file, header = TRUE, sep = "\t", stringsAsFactors = FALSE)
  key_old <- paste(normalize_key(existing$Region),
                   ifelse(is.na(existing$Code), "", existing$Code),
                   normalize_key(existing$Status),
                   existing$EmailTimestamp, sep = "|")
  key_new <- paste(normalize_key(temp_df$Region),
                   ifelse(is.na(temp_df$Code), "", temp_df$Code),
                   normalize_key(temp_df$Status),
                   temp_df$EmailTimestamp, sep = "|")
  to_add <- temp_df[!(key_new %in% key_old), , drop = FALSE]
  if (nrow(to_add)) {
    write.table(to_add, file = history_file, append = TRUE, sep = "\t",
                row.names = FALSE, col.names = FALSE, quote = FALSE)
  } else {
    message("No new rows to append to history.")
  }
  invisible(TRUE)
}

as_char_df <- function(df) data.frame(lapply(df, as.character), stringsAsFactors = FALSE)

# ──────────────────────────────────────────────────────────────────────────────
# File bootstrapping
# ──────────────────────────────────────────────────────────────────────────────
if (!file.exists(LOG_FILE)) {
  file.create(LOG_FILE)
}

if (!file.exists(HISTORY_FILE)) {
  history_header <- data.frame(
    Region = character(), Code = character(), Status = character(),
    EmailTimestamp = character(), EmailID = character(), stringsAsFactors = FALSE
  )
  write.table(history_header, file = HISTORY_FILE, sep = "\t", row.names = FALSE,
              col.names = TRUE, quote = FALSE)
}

if (!file.exists(ACTIVE_FILE)) {
  active_header <- data.frame(
    Region = character(), Code = character(), Status = character(),
    EmailTimestamp = character(), EmailID = character(), stringsAsFactors = FALSE
  )
  write.table(active_header, file = ACTIVE_FILE, sep = "\t", row.names = FALSE,
              col.names = TRUE, quote = FALSE)
}

# Load current active alerts (as character columns)
active_alerts <- if (file.exists(ACTIVE_FILE)) {
  as_char_df(read.table(ACTIVE_FILE, header = TRUE, sep = "\t", stringsAsFactors = FALSE))
} else {
  as_char_df(data.frame(
    Region = character(), Code = character(), Status = character(),
    EmailTimestamp = character(), EmailID = character(), stringsAsFactors = FALSE
  ))
}

# ──────────────────────────────────────────────────────────────────────────────
# IMAP connect & search
# ──────────────────────────────────────────────────────────────────────────────
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

# Yahoo typically expects "INBOX"
con$select_folder(name = "INBOX")

emails <- con$search_string(
  expr = "Alert for AQ Alerts",
  where = "SUBJECT"
)

# Deduplicate via processed log (treat IDs as character)
emails_chr     <- as.character(emails)
processed_ids  <- if (file.exists(LOG_FILE)) readLines(LOG_FILE) else character(0)
new_emails     <- emails_chr[!emails_chr %in% processed_ids]

# IMAP runtime knobs
con$reset_timeout_ms(x = 30000)
con$reset_verbose(x = FALSE)

# ──────────────────────────────────────────────────────────────────────────────
# Process new emails
# ──────────────────────────────────────────────────────────────────────────────
all_updates <- list()

if (length(new_emails)) {
  for (email_id in new_emails) {
    # Full RFC822 message → parse MIME → extract text/plain
    raw_msg  <- con$fetch_msg(msg_id = email_id)
    eml      <- mime::parse_email(raw_msg)
    plain_tx <- get_text_plain(eml)
    
    if (is.null(plain_tx) || !nzchar(plain_tx)) {
      message("No text/plain body found for email ID: ", email_id, " — skipping.")
      write(email_id, file = LOG_FILE, append = TRUE)
      next
    }
    
    temp_df <- extract_info(raw_msg, plain_tx, email_id)
    if (is.null(temp_df) || !nrow(temp_df)) {
      write(email_id, file = LOG_FILE, append = TRUE)
      next
    }
    
    # Expand umbrella regions → specific sub-regions
    temp_df <- expand_umbrella_regions(temp_df)
    
    # Append to history (duplicate-safe)
    append_new_history(temp_df, HISTORY_FILE)
    
    # Accumulate updates for active-alert merge
    all_updates[[length(all_updates) + 1]] <- temp_df
    
    # Mark as processed
    write(email_id, file = LOG_FILE, append = TRUE)
  }
} else {
  message("No new emails to process.")
}

# ──────────────────────────────────────────────────────────────────────────────
# Merge updates into active_alerts
# ──────────────────────────────────────────────────────────────────────────────
if (length(all_updates)) {
  updates_df <- do.call(rbind, all_updates)
  
  for (i in seq_len(nrow(updates_df))) {
    row    <- updates_df[i, ]
    action <- tolower(row$Status)
    
    # Prefer matching by Code when present; else by normalized Region
    if (!is.na(row$Code) && nzchar(row$Code)) {
      match_idx <- which(active_alerts$Code == row$Code)
    } else {
      rk <- normalize_key(row$Region)
      match_idx <- which(normalize_key(active_alerts$Region) == rk)
    }
    
    if (identical(action, "ended")) {
      if (length(match_idx)) {
        message("Ending alert for: ", row$Region, " | Code: ", row$Code)
        active_alerts <- active_alerts[-match_idx, , drop = FALSE]
      }
    } else if (action %in% c("issued", "continued")) {
      if (length(match_idx)) {
        active_alerts[match_idx, c("EmailTimestamp","Status","EmailID")] <- 
          as_char_df(row[c("EmailTimestamp","Status","EmailID")])
      } else {
        active_alerts <- rbind(active_alerts, as_char_df(row))
      }
    }
  }
  
  # Save updated active alerts (overwrite)
  write.table(active_alerts, file = ACTIVE_FILE, sep = "\t", row.names = FALSE,
              col.names = TRUE, quote = FALSE)
}

# ──────────────────────────────────────────────────────────────────────────────
# Build output JSON (booleans per target region)
# ──────────────────────────────────────────────────────────────────────────────
clean_region <- function(x) gsub("\\s+", " ", trimws(x))

active_regions_norm <- normalize_key(active_alerts$Region)
targets_norm        <- normalize_key(REGIONS_FOR_JSON)

advisories_list <- lapply(seq_along(REGIONS_FOR_JSON), function(i) {
  list(
    Region = REGIONS_FOR_JSON[i],
    ActiveAlert = targets_norm[i] %in% active_regions_norm
  )
})

final_json <- list(
  lastChecked = format(with_tz(Sys.time(), tzone = TIMEZONE_OUT), "%Y-%m-%d %H:%M:%S"),
  Advisories  = advisories_list
)

write_json(final_json, path = OUTPUT_JSON, pretty = TRUE, auto_unbox = TRUE)

message("Done. Wrote: ", OUTPUT_JSON)
