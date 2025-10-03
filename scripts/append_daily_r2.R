#!/usr/bin/env Rscript
# Wrapper: forwards args to append_to_r2_csv.R and injects input-template + date-col

args <- commandArgs(trailingOnly = TRUE)

get_val <- function(flag, default = "") {
  i <- which(args == flag)
  if (!length(i)) return(default)
  if (i[1] == length(args)) stop(paste("Missing value for", flag))
  args[i[1] + 1]
}

# Use focus-date if provided by the workflow, else compute yesterday in America/Vancouver
focus <- get_val("--focus-date", "")
if (!nzchar(focus)) {
  tz <- "America/Vancouver"
  now <- as.POSIXct(Sys.time(), tz = tz)
  yest <- as.Date(now, tz = tz) - 1
  focus <- format(yest, "%Y_%m_%d")
}

# Build daily input template per sensor (adjust if your filenames differ)
input_tmpl <- paste0("calibrated_data/{sensor}/", focus, ".csv")
# If you prefer a rolling file, use:
# input_tmpl <- "calibrated_data/{sensor}/latest.csv"

# Drop --focus-date before forwarding
drop_pair <- function(vec, flag) {
  i <- which(vec == flag)
  if (!length(i)) return(vec)
  if (i[1] < length(vec)) vec <- vec[-c(i[1], i[1] + 1)] else vec <- vec[-i[1]]
  vec
}
pass <- drop_pair(args, "--focus-date")

cmd <- c("scripts/append_to_r2_csv.R",
         pass,
         "--input-template", input_tmpl,
         "--date-col", "DATE")

status <- system2("Rscript", cmd)
quit(status = ifelse(is.null(status), 0, status))
