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
