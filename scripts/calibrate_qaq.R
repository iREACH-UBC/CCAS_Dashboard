#!/usr/bin/env Rscript
# ─── calibrate_qaq.R ────────────────────────────────────────────
# Calibrate QuantAQ MOD-* sensor data using the same CAPS models
# & AQHI pipeline used for RAMPs. Produces a 24h PST window
# ending "now" and writes a per-sensor CSV in calibrated_data/<sid>/.
#
# Usage: Rscript scripts/calibrate_qaq.R MOD-00616
# If no args -> fallback vector (handy for local dev).

suppressPackageStartupMessages({
  library(dplyr);  library(readr);  library(lubridate);  library(stringr)
  library(purrr);  library(tibble); library(fs);          library(zoo)
  library(glue);   library(gtools); library(tidyr)
})

# ---------------------------------------------------------------
# Args / config
# ---------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
fallback_ids <- c("MOD-00616","MOD-00632","MOD-00625","MOD-00631",
                  "MOD-00623","MOD-00628","MOD-00620","MOD-00627",
                  "MOD-00630","MOD-00624")
sensor_ids <- if (length(args)) args else fallback_ids

data_folder   <- "data"
output_folder <- "calibrated_data"
dir_create(output_folder)

model_path <- Sys.getenv("CAL_MODEL_PATH")
if (identical(model_path, "") || !file.exists(model_path))
  stop("CAL_MODEL_PATH env-var not set or file missing – aborting run.")

# ---------------------------------------------------------------
# Load CAPS calibration models
# ---------------------------------------------------------------
obj_names <- load(model_path)
if (!"Calibration_Models" %in% obj_names) {
  Calibration_Models <- get(obj_names[1], envir = .GlobalEnv)
}
# Inspect: names(Calibration_Models)
# Expected elements: pollutant-level models + helper funcs used below.

# If your model object names differ, edit these two helpers --------------------
# choose_model(pollutant) → returns model from Calibration_Models
# apply_caps_to_qaq(df_raw) → returns tibble with calibrated cols

choose_model <- function(pollutant) {
  # Common pattern: Calibration_Models[[pollutant]] or with suffixes
  nm <- pollutant
  if (!nm %in% names(Calibration_Models)) return(NULL)
  Calibration_Models[[nm]]
}

# You already have CAPS_Hybrid_Apply / CAPS_PR_Apply in your calibration repo.
# We’ll try hybrid first, then parametric (PR) if hybrid absent.
apply_caps_to_series <- function(raw_vec, model) {
  # generic wrapper; update if model class requires special call
  if (is.null(model)) return(raw_vec)  # passthrough if no model
  # Example:
  if (exists("CAPS_Hybrid_Apply", mode = "function")) {
    tryCatch(return(CAPS_Hybrid_Apply(raw_vec, model)),
             error = function(e) raw_vec)
  }
  if (exists("CAPS_PR_Apply", mode = "function")) {
    tryCatch(return(CAPS_PR_Apply(raw_vec, model)),
             error = function(e) raw_vec)
  }
  raw_vec
}

# Top-level calibrator: takes raw QAQ df w/ standardized cols, returns calibrated tibble
apply_caps_to_qaq <- function(df_std) {
  # loop pollutants we care about
  pols <- c("CO","NO","NO2","O3","CO2","T","RH","PM1.0","PM2.5","PM10")
  for (p in pols) {
    m <- choose_model(p)
    if (!is.null(m) && p %in% names(df_std)) {
      df_std[[p]] <- apply_caps_to_series(df_std[[p]], m)
    }
  }
  df_std
}

# ---------------------------------------------------------------
# Variable mapping: QAQ raw -> internal standard columns
# Update if your raw header uses underscores not dots, etc.
# ---------------------------------------------------------------
varmap <- c(
  "co"   = "CO",
  "no"   = "NO",
  "no2"  = "NO2",
  "o3"   = "O3",
  "co2"  = "CO2",
  "temp" = "T",
  "rh"   = "RH",
  "pm1"  = "PM1.0",
  "pm25" = "PM2.5",
  "pm10" = "PM10"
)

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------
apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()

# 24h window (PST)
tz_local  <- "America/Los_Angeles"
now_pst   <- with_tz(Sys.time(), tz_local)
past_24h  <- now_pst - hours(24)

# column order we promise downstream
desired_cols <- c("DATE","TE","CO","NO","NO2","O3","CO2","T","RH",
                  "PM1.0","PM2.5","PM10","AQHI","Top_AQHI_Contributor")

# In QAQ files, dates appear in basename: <sid>-YYYY-MM-DD*.csv
extract_date <- function(paths)
  as.Date(str_match(path_file(paths), "\\d{4}-\\d{2}-\\d{2}")[,1])

# read & standardize a single raw file -------------------------
read_qaq_raw <- function(path) {
  df <- suppressWarnings(read_csv(path, show_col_types = FALSE))
  if (!"timestamp_local" %in% names(df))
    stop("timestamp_local missing in ", path)

  df <- df %>%
    mutate(DATE = ymd_hms(timestamp_local, tz = tz_local, quiet = TRUE)) %>%
    select(-timestamp_local)

  # rename known vars
  for (raw in names(varmap)) {
    if (raw %in% names(df))
      df <- df %>% rename(!!varmap[[raw]] := all_of(raw))
  }

  # ensure all expected numeric columns exist
  for (nm in setdiff(unname(varmap), names(df)))
    df[[nm]] <- NA_real_

  df
}

# compute AQHI & top contributor (as in RAMP pipeline) ----------
add_aqhi <- function(df) {
  df <- df %>% arrange(DATE)
  # 3h moving means: 12 * 15min? we don't know QAQ timestep; use rollapply width by time
  # Safer: use zoo::rollapplyr index by 12 obs if 15min; but QAQ may be 5min.
  # Use dplyr grouped rolling over time with slider? For simplicity, use
  # lubridate floor + rolling windows via runner package? We'll approximate using time-based rollapply via zoo.

  # convert to zoo object with indexed POSIXct
  z <- zoo(df$NO2, df$DATE)
  NO2_3h  <- rollapply(z, width = as.difftime(3, units="hours"), FUN = mean,
                       align = "right", partial = TRUE, na.rm = TRUE)
  z <- zoo(df$O3,  df$DATE)
  O3_3h   <- rollapply(z, width = as.difftime(3, units="hours"), FUN = mean,
                       align = "right", partial = TRUE, na.rm = TRUE)
  z <- zoo(df$PM2.5,df$DATE)
  PM25_3h <- rollapply(z, width = as.difftime(3, units="hours"), FUN = mean,
                       align = "right", partial = TRUE, na.rm = TRUE)
  PM25_1h <- rollapply(z, width = as.difftime(1, units="hours"), FUN = mean,
                       align = "right", partial = TRUE, na.rm = TRUE)

  # align back
  df$NO2_3h  <- as.numeric(NO2_3h[df$DATE])
  df$O3_3h   <- as.numeric(O3_3h[df$DATE])
  df$PM25_3h <- as.numeric(PM25_3h[df$DATE])
  df$PM25_1h <- as.numeric(PM25_1h[df$DATE])

  contrib_sum <- with(df,
                      (exp(0.000871 * NO2_3h) - 1) +
                      (exp(0.000537 * O3_3h)  - 1) +
                      (exp(0.000487 * PM25_3h) - 1))

  df <- df %>%
    mutate(
      AQHI_raw = (10 / 10.4) * 100 * (
        (exp(0.000871 * NO2_3h) - 1) +
        (exp(0.000537 * O3_3h)  - 1) +
        (exp(0.000487 * PM25_3h) - 1)
      ),
      AQHI        = apply_aqhi_ceiling(AQHI_raw, PM25_1h),
      NO2_contrib = (exp(0.000871 * NO2_3h)  - 1) / contrib_sum,
      O3_contrib  = (exp(0.000537 * O3_3h)   - 1) / contrib_sum,
      PM25_contrib= (exp(0.000487 * PM25_3h) - 1) / contrib_sum,
      Top_AQHI_Contributor = pmap_chr(
        list(NO2_contrib, O3_contrib, PM25_contrib),
        function(no2, o3, pm25) {
          vals <- c(NO2 = no2, O3 = o3, `PM2.5` = pm25)
          if (all(is.na(vals))) NA_character_
          else names(vals)[which.max(replace_na(vals, -Inf))]
        })
    ) %>%
    select(-ends_with("_3h"), -PM25_1h, -AQHI_raw,
           everything(), AQHI, Top_AQHI_Contributor) # ensure AQHI cols kept

  df
}

# fallback single row if sensor stale -----------------------------------------
fallback_row <- function() {
  tibble(
    DATE = now_pst,
    TE   = "QAQ",
    CO=-1,NO=-1,NO2=-1,O3=-1,CO2=-1,T=-1,RH=-1,
    `PM1.0`=-1,`PM2.5`=-1,PM10=-1,
    AQHI=-1,
    Top_AQHI_Contributor = "-1"
  )
}

# ---------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------
for (sid in sensor_ids) {
  message("── QAQ Sensor ", sid, " ────────────────────────────")

  # files for yesterday+today PST
  date_window <- seq.Date(as_date(now_pst) - 1, as_date(now_pst), by = "day")
  target_files <- glue("{sid}_{format(date_window, '%Y-%m-%d')}")
  # match prefix because QuantAQ filenames may include suffixes (raw/processed)
  # ── robust file discovery ──────────────────────────────────────
  # • **/ prefix ⇒ match at any depth under data/
  # • no further filtering needed; we’ll sort by date in the filename
  all_paths <- dir_ls(
    data_folder,
    recurse = TRUE,
    type    = "file",
    glob    = glue("**/{sid}-*.csv")   # <── key fix
  )
  
  files_raw <- all_paths
  

  if (!length(files_raw)) {
    warning("No raw QAQ data for ", sid)
    df_out <- fallback_row()
  } else {
    # tibble & sort newest->oldest by date parsed from name
    files_tbl <- tibble(path = files_raw,
                        date_file = extract_date(files_raw)) %>%
      filter(!is.na(date_file)) %>%
      arrange(desc(date_file))

    # read & calibrate until ≥24h coverage
    parts <- list(); earliest <- now_pst
    for (p in files_tbl$path) {
      message("  • reading ", path_file(p))
      raw <- tryCatch(read_qaq_raw(p), error = function(e) {
        warning("    read failed: ", conditionMessage(e)); NULL
      })
      if (is.null(raw)) next
      # apply CAPS models
      cal <- apply_caps_to_qaq(raw)
      
      cal <- cal %>%
        mutate(DATE = lubridate::floor_date(DATE, "15 minutes")) %>%  # bucket start
        group_by(DATE) %>%
        summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE)),
                  .groups = "drop")
      
      parts <- append(parts, list(cal))
      earliest <- min(earliest, min(cal$DATE, na.rm = TRUE))
      if (earliest <= past_24h) break
    }

    if (!length(parts)) {
      warning("  • no calibrated rows after processing ", sid)
      df_out <- fallback_row()
    } else {
      df_all <- bind_rows(parts) %>%
        mutate(
          DATE = with_tz(DATE, tz_local) + hours(2),  # device clock offset to align w/ RAMPs
          TE   = "QAQ"
        ) %>%
        arrange(DATE) %>%
        filter(DATE >= past_24h)

      if (nrow(df_all) == 0) {
        message("  • all data stale (<24h) – fallback row.")
        df_out <- fallback_row()
      } else {
        df_out <- df_all %>% add_aqhi()

        # ensure required columns exist & order
        for (col in desired_cols)
          if (!col %in% names(df_out)) df_out[[col]] <- NA_real_
        df_out <- df_out %>% select(all_of(desired_cols))
      }
    }
  }

  # write -------------------------------------------------------
  sensor_dir <- path(output_folder, sid); dir_create(sensor_dir)
  first_day <- format(min(df_out$DATE, na.rm = TRUE), "%Y_%m_%d")
  last_day  <- format(now_pst, "%Y_%m_%d")
  outfile <- path(sensor_dir, glue("{sid}_calibrated_{first_day}_to_{last_day}.csv"))
  write_csv(df_out, outfile, na = "")
  write_lines(as.character(now_pst), path(sensor_dir, "LAST_RUN.txt"))
  message("  ✔ wrote ", outfile, " (", nrow(df_out), " rows)")
}


