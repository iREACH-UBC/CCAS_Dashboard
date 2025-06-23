# ─── calibrate_data.R ─────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(dplyr);  library(readr);  library(lubridate);  library(stringr)
  library(purrr);  library(tibble); library(fs);          library(zoo)
  library(glue);   library(gtools); library(tidyr)
})

source("scripts/apply_caps_calibration.R")

# ── CONFIG --------------------------------------------------------------------
sensor_ids    <- c("2021")                 # expand as needed
data_folder   <- "data"                    # raw logger exports
output_folder <- "calibrated_data"         # per-sensor sub-folders
dir_create(output_folder)

# where the GitHub Action stored the downloaded .obj
model_path <- Sys.getenv("CAL_MODEL_PATH")
if (model_path == "" || !file.exists(model_path))
  stop("CAL_MODEL_PATH env-var not set or file missing – aborting run.")

# ── helper: extract Date from "<sid>_YYYY_MM_DD.csv" --------------------------
extract_date <- function(paths) {
  files    <- path_file(paths)
  date_txt <- str_extract(files, "\\d{4}-\\d{2}-\\d{2}")
  as.Date(date_txt, format = "%Y-%m-%d")
}

# ── helper: AQHI ≥ AQHI-Plus ceiling -----------------------------------------
apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec) {
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |>
    as.integer()
}

# ── MAIN LOOP -----------------------------------------------------------------
now_pst  <- with_tz(Sys.time(), "America/Los_Angeles")
past_24h <- now_pst - hours(24)

for (sid in sensor_ids) {
  message("── Sensor ", sid, " ────────────────────────────────────────────")
  
  # (1) find the two newest raw files -----------------------------------------
  files_raw <- list.files(
    path        = data_folder,
    pattern     = paste0("^", sid, "_\\d{4}-\\d{2}-\\d{2}\\.csv$"),
    recursive   = TRUE,
    full.names  = TRUE,
    ignore.case = TRUE
  )
  message("  • found ", length(files_raw), " raw file(s)")
  if (length(files_raw) == 0) next
  
  last_two <- tibble(path = files_raw, date = extract_date(files_raw)) |>
    filter(!is.na(date)) |>
    arrange(date) |>
    slice_tail(n = 2) |>
    pull(path)
  
  message("  • taking files: ", paste(path_file(last_two), collapse = ", "))
  
  # (2) calibrate both files ---------------------------------------------------
  calibrated <- map(
    last_two,
    ~ apply_caps_calibration(
      sensor_id  = sid,
      data_file  = .x,
      model_path = model_path            # <<< NEW
    )
  ) |>
    bind_rows()
  
  if (nrow(calibrated) == 0) {
    message("  • no calibrated rows – skipping"); next
  }
  
  # (3) tidy + 24 h filter -----------------------------------------------------
  calib <- calibrated |>
    mutate(
      DATE = with_tz(date, "America/Los_Angeles") + hours(2),
      .keep = "unused"
    ) |>
    rename(PM2.5 = PM2_5) |>
    select(DATE, CO, NO, NO2, O3, CO2, PM2.5, everything()) |>
    filter(DATE >= past_24h)
  
  # (4) rolling means, AQHI, contributors -------------------------------------
  calib <- calib |>
    arrange(DATE) |>
    mutate(
      NO2_3h  = rollapply(NO2,     12, mean, fill = NA, align = "right", na.rm = TRUE),
      O3_3h   = rollapply(O3,      12, mean, fill = NA, align = "right", na.rm = TRUE),
      PM25_3h = rollapply(PM2.5,   12, mean, fill = NA, align = "right", na.rm = TRUE),
      PM25_1h = rollapply(PM2.5,    4, mean, fill = NA, align = "right", na.rm = TRUE),
      AQHI_raw = (10 / 10.4) * 100 * (
        (exp(0.000871 * NO2_3h) - 1) +
          (exp(0.000537 * O3_3h)  - 1) +
          (exp(0.000487 * PM25_3h) - 1)
      )
    )
  
  contrib_sum <- with(calib,
                      (exp(0.000871 * NO2_3h) - 1) +
                        (exp(0.000537 * O3_3h)  - 1) +
                        (exp(0.000487 * PM25_3h) - 1)
  )
  
  calib <- calib |>
    mutate(
      AQHI         = apply_aqhi_ceiling(AQHI_raw, PM25_1h),
      NO2_contrib  = (exp(0.000871 * NO2_3h)  - 1) / contrib_sum,
      O3_contrib   = (exp(0.000537 * O3_3h)   - 1) / contrib_sum,
      PM25_contrib = (exp(0.000487 * PM25_3h) - 1) / contrib_sum,
      Top_AQHI_Contributor = pmap_chr(
        list(NO2_contrib, O3_contrib, PM25_contrib),
        function(no2, o3, pm25) {
          vals <- c(NO2 = no2, O3 = o3, `PM2.5` = pm25)
          if (all(is.na(vals))) NA_character_
          else names(vals)[which.max(replace_na(vals, -Inf))]
        }
      )
    ) |>
    select(-ends_with("_3h"), -PM25_1h, -AQHI_raw)
  
  # (5) write out -------------------------------------------------------------
  sensor_dir <- path(output_folder, sid)
  dir_create(sensor_dir)
  
  outfile <- path(
    sensor_dir,
    glue("{sid}_calibrated_{format(min(extract_date(last_two)), '%Y_%m_%d')}_to_{format(max(extract_date(last_two)), '%Y_%m_%d')}.csv")
  )
  write_csv(calib, outfile, na = "")
  message("  ✔ wrote ", outfile)
}
