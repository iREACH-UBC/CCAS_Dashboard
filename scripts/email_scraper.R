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

# list all folders in the mailbox
folders <- con$list_mail_folders()

con$select_folder(name = "Inbox")

emails <- con$search_string(
  expr = "Alert for AQ Alerts",
  where = "SUBJECT"
)

emails # prints IDs of  emails with string "Alert for AQ Alerts" in the subject line

# set up a log file of previously processed emails
log_file <- "processed_emails.txt"
processed_ids <- if (file.exists(log_file)) {
  readLines(log_file)
} else {
  character(0)
}

# failsafe if log_file doesn't exist
if (!file.exists(log_file)) {
  file.create(log_file)
}

# filters out processed emails
new_emails <- emails[!emails %in% processed_ids]

# extracts data from unprocessed emails
con$reset_timeout_ms(x = 30000) # increasing timeout, recommended by documentation

# if verbose = TRUE before, sets to FALSE so the process is faster
# initially had it set to verbose to get info about why it wasn't connecting
con$reset_verbose(x = FALSE)


# FETCHING REGION AND ALERT STATUS

# Function: removes line breaks, newlines, + encoded characters from the email plain text
clean_plain_text <- function(text) {
  text <- gsub("=\\r\\n|=\\n", "", text)  # remove soft breaks
  text <- gsub("\\r\\n|\\r", "\n", text) # normalize newlines
  text <- gsub("=([A-F0-9]{2})", "", text) # remove encoded characters
  text <- gsub("\n{3,}", "\n\n", text)    # get rid of blank lines
  text <- trimws(text)
  return(text)
}

# Function: extracts the advisory timestamp, advisory status, location, and region code
extract_info <- function(raw_mime, plain_text, email_id) {
  # Extract email timestamp from raw mime
  time_of_issue <- str_match(raw_mime, "Issued at ([0-9\\-]{10}\\s+[0-9:APM ]+\\s+[A-Z]{3})")
  
  if (is.na(time_of_issue[1, 2])) {
    warning("No 'Issued at' timestamp found.")
    return(Sys.time())  # fallback to current time
  }
  
  datetime_str <- time_of_issue[1, 2]
  
  # Extract all blocks of the form: "air quality ... - (status) for:"
  alert_matches <- str_match_all(
    plain_text,
    "(?i)air quality (warning|statement) - (issued|ended|continued) for:\\s*([\\s\\S]+?)(?=\\n{2}|The above alert|Current details|Air quality (warning|statement))"
  )[[1]]
  
  if (nrow(alert_matches) == 0) {
    message("No alert found for email ID: ", email_id)
    return(NULL)
  }
  
  all_dfs <- list()
  
  for (j in seq_len(nrow(alert_matches))) {
    status <- tolower(alert_matches[j, 3])
    locations_list <- alert_matches[j, 4]
    
    locations <- str_trim(unlist(strsplit(locations_list, "\n")))
    locations <- locations[locations != ""]
    
    location_names <- gsub("\\s*\\(.*\\)", "", locations)
    codes <- str_extract(locations, "(?<=\\()[^)]+(?=\\))")
    
    min_len <- min(length(location_names), length(codes))
    if (min_len == 0) next
    
    temp_df <- data.frame(
      Region = location_names[1:min_len],
      Code = codes[1:min_len],
      Status = status,
      EmailTimestamp = datetime_str,
      EmailID = email_id,
      stringsAsFactors = FALSE
    )
    
    temp_df <- temp_df[!(is.na(temp_df$Region) | is.na(temp_df$Code)), ]
    
    if (nrow(temp_df) > 0) {
      all_dfs[[length(all_dfs) + 1]] <- temp_df
    }
  }
  
  if (length(all_dfs) == 0) {
    warning("No valid region/code pairs found in email ID: ", email_id)
    return(NULL)
  }
  
  return(do.call(rbind, all_dfs))
}



# Initialize files to keep track of active alerts and alert history
active_file <- "active_alerts.txt"
history_file <- "alert_history.txt"

# Ensure history file exists with proper header
if (!file.exists(history_file)) {
  history_header <- data.frame(
    Region = character(),
    Code = character(),
    Status = character(),
    EmailTimestamp = character(),
    EmailID = character(),
    stringsAsFactors = FALSE
  )
  write.table(history_header, file = history_file, sep = "\t", row.names = FALSE,
              col.names = TRUE, quote = FALSE)
}

# Ensure active alerts file exists with proper header
if (!file.exists(active_file)) {
  active_header <- data.frame(
    Region = character(),
    Code = character(),
    Status = character(),
    EmailTimestamp = character(),
    EmailID = character(),
    stringsAsFactors = FALSE
  )
  write.table(active_header, file = active_file, sep = "\t", row.names = FALSE,
              col.names = TRUE, quote = FALSE)
}

# Function: searches alerts that have already been added to historical/active files
# needed to add this bc every time I ran the code it would add the same emails to the logs

append_new_history <- function(temp_df, history_file) {
  if (file.exists(history_file)) {
    existing_history <- read.table(history_file, header = TRUE, sep = "\t", stringsAsFactors = FALSE)
    # create unique keys from all columns to check duplicates
    existing_keys <- apply(existing_history, 1, paste, collapse = "_")
    new_keys <- apply(temp_df, 1, paste, collapse = "_")
    to_append <- temp_df[!(new_keys %in% existing_keys), ]
    
    if (nrow(to_append) > 0) {
      write.table(to_append, file = history_file, append = TRUE, sep = "\t",
                  row.names = FALSE, col.names = FALSE, quote = FALSE)
    } else {
      message("No new rows to append to history.")
    }
  } else {
    # file doesn't exist, write full dataframe with header
    write.table(temp_df, file = history_file, append = FALSE, sep = "\t",
                row.names = FALSE, col.names = TRUE, quote = FALSE)
  }
}



# initialize active alerts from file or empty df
if (file.exists(active_file)) {
  active_alerts <- read.table(active_file, header = TRUE, sep = "\t", stringsAsFactors = FALSE)
  active_alerts$Code <- as.character(active_alerts$Code)
} else {
  active_alerts <- data.frame(
    Region = character(),
    Code = character(),
    Status = character(),
    EmailTimestamp = character(),
    EmailID = character(),
    stringsAsFactors = FALSE
  )
}

all_updates <- list()

for (email_id in new_emails) {
  raw_mime <- con$fetch_text(msg_id = email_id)
  plain_text <- str_extract(
    raw_mime, 
    "(?s)(?<=Content-Type: text/plain; charset=utf-8\\r\\n\\r\\n).*?(?=--_)"
  )
  plain_text <- clean_plain_text(plain_text)
  
  temp_df <- extract_info(raw_mime, plain_text, email_id)
  print(temp_df)
  
  if (!is.null(temp_df)) {
    # append to full alert history, avoiding duplicates
    append_new_history(temp_df, history_file)
    
    all_updates[[length(all_updates) + 1]] <- temp_df
    
    write(email_id, file = log_file, append = TRUE)
  }
}


if (length(all_updates) > 0) {
  updates_df <- do.call(rbind, all_updates)
  
  for (i in seq_len(nrow(updates_df))) {
    row <- updates_df[i, ]
    status <- row$Status
    
    if (status == "ended") {
      # check if alerts have ended
      code_region_match <- active_alerts$Code == row$Code & active_alerts$Region == row$Region
      
      # debugging to confirm correct removal
      message("Removing ended alert for: ", row$Region, " | Code: ", row$Code)
      
      # remove ended alerts
      active_alerts <- active_alerts[!code_region_match, , drop = FALSE]
      
      
    } else if (status %in% c("issued", "continued")) {
      code_region_match <- active_alerts$Code == row$Code & active_alerts$Region == row$Region
      
      if (any(is.na(active_alerts$Code))) {
        warning("NA values found in active_alerts$Code")
      }
      
      if (is.na(row$Code)) {
        warning("NA found in row$Code for region: ", row$Region)
      }
      

      if (any(code_region_match)) {
        # update existing alert
        active_alerts[code_region_match, "EmailTimestamp"] <- row$EmailTimestamp
        active_alerts[code_region_match, "Status"] <- row$Status
        active_alerts[code_region_match, "EmailID"] <- row$EmailID
        
      } else {
        # Add new alert
        active_alerts <- rbind(active_alerts, row)
      }
    }
  }
  
  # Save updated active alerts (overwrite, no append)
  write.table(active_alerts, file = active_file, sep = "\t", row.names = FALSE,
              col.names = TRUE, quote = FALSE)
}

active_regions <- active_alerts$Region
#  status <- str_extract(plain_text, "(?i)statement (issued|ended)")
#  if (tolower(status) == "issued") {
#    issued_regions <- location_names
#  }
#}

# Define all target regions
regions <- c(
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
  "Fraser Canyon - south including Lytton, B.C.",
  "Lake Winnipegosis, Man.",
  "The Pas Cormorant Westray and Wanless, Man.",
  "Westgate Red Deer Lake and Barrows, Man.",
  "Porcupine Prov. Forest, Man.",
  "Thompson Thicket Portage and Pikwitonei, Man.",
  "Nelson House, Man.",
  "Flin Flon Cranberry Portage and Grass River Prov. Park, Man."
)

# Build output list
clean_region <- function(x) gsub("\\s+", " ", trimws(x))

active_regions <- clean_region(active_alerts$Region)
regions_cleaned <- clean_region(regions)

output <- lapply(seq_along(regions), function(i) {
  list(
    Region = regions[i],
    ActiveAlert = regions_cleaned[i] %in% active_regions
  )
})


# Add timestamp
final_json <- list(
  lastChecked = format(with_tz(Sys.time(), tzone = "America/Vancouver"), "%Y-%m-%d %H:%M:%S"),
  Advisories = output
)

# Write JSON
write_json(final_json, path = "AQAdvisories.json", pretty = TRUE, auto_unbox = TRUE)
