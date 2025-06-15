# ─── process_sensors.R ─────────────────────────────────────────────────────────
library(tidyverse)            # dplyr, readr, lubridate, stringr, purrr, tibble…
library(fs)
library(zoo)                  # rollapply
library(glue)                 # glue::glue()
source("scripts/apply_caps_calibration.R")   # wrapper we wrote earlier

# ── CONFIG --------------------------------------------------------------------
sensor_ids    <- c("2021","2022","2023","2024","2026","2030","2031","2032",
                   "2033","2034","2039","2040","2041","2042","2043")
data_folder   <- "data"              # raw logger exports live here
output_folder <- "calibrated_data"   # sensor sub-folders written here
dir_create(output_folder)

# ── helper: extract Date from "<sid>_YYYY_MM_DD.csv" --------------------------
extract_date <- function(paths) {
  files    <- fs::path_file(paths)                       # keep just the names
  date_txt <- stringr::str_extract(files,                # pick yyyy-mm-dd
                                   "\\d{4}-\\d{2}-\\d{2}")
  as.Date(date_txt, format = "%Y-%m-%d")                 # returns a Date vector
}


# ── helper: AQHI ≥ AQHI-Plus ceiling -----------------------------------------
apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec) {
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()
}

# ── MAIN LOOP -----------------------------------------------------------------
now_pst  <- lubridate::with_tz(Sys.time(), tzone = "America/Los_Angeles")
past_24h <- now_pst - lubridate::hours(24)

for (sid in sensor_ids) {
  message("── Sensor ", sid, " ────────────────────────────────────────────")
  
  # ── (1) find raw files – recursive search so any sub-folder works
  files_raw <- list.files(
    path        = data_folder,
    pattern     = paste0("^", sid, "_\\d{4}-\\d{2}-\\d{2}\\.csv$"),
    recursive   = TRUE,      # keep TRUE if files may be in sub-folders
    full.names  = TRUE,
    ignore.case = TRUE
  )
  
  message("  • found ", length(files_raw), " raw file(s)")
  
  if (length(files_raw) == 0) {
    message("  • no raw files found – skipping"); next
  }
  
  # ── (2) parse dates, keep well-formed, pick two newest ---------------------
  file_df <- tibble(
    path = files_raw,
    date = extract_date(files_raw)
  ) |>
    filter(!is.na(date))
  
  if (nrow(file_df) < 2) {
    message("  • fewer than two parsable files – skipping"); next
  }
  
  last_two <- file_df |>
    arrange(date) |>
    slice_tail(n = 2) |>
    pull(path)
  
  message("  • taking files: ", paste(path_file(last_two), collapse = ", "))
  
  # ── (3) run calibration wrapper on each file → list of tibbles ------------
  calibrated_list <- map(last_two, ~ apply_caps_calibration(
    sensor_id = sid, data_file = .x,
    model_root = "calibration_models",
    out_dir    = NULL
  ))
  
  # ── (4) merge & tidy -------------------------------------------------------
  calib <- bind_rows(calibrated_list) |>
    mutate(
      DATE  = lubridate::with_tz(date, "America/Los_Angeles"),
      .keep = "unused"
    ) |>
    rename(`PM2.5` = PM2_5) |>
    select(DATE, CO, NO, NO2, O3, CO2, `PM2.5`, everything()) |>
    filter(DATE >= past_24h)
  
  if (nrow(calib) == 0) {
    message("  • no calibrated data in the past 24 h – skipping"); next
  }
  
  # ── (5) rolling means ------------------------------------------------------
  calib <- calib |>
    arrange(DATE) |>
    mutate(
      NO2_3h   = zoo::rollapply(NO2,     12, mean, fill = NA, align = "right", na.rm = TRUE),
      O3_3h    = zoo::rollapply(O3,      12, mean, fill = NA, align = "right", na.rm = TRUE),
      PM25_3h  = zoo::rollapply(`PM2.5`, 12, mean, fill = NA, align = "right", na.rm = TRUE),
      PM25_1h  = zoo::rollapply(`PM2.5`,  4, mean, fill = NA, align = "right", na.rm = TRUE),
      AQHI_raw = (10/10.4) * 100 * (
        (exp(0.000871 * NO2_3h)  - 1) +
          (exp(0.000537 * O3_3h)   - 1) +
          (exp(0.000487 * PM25_3h) - 1)
      )
    )
  
  # ── (6) AQHI-Plus ceiling + contributor shares ----------------------------
  contrib_sum <- with(calib,
                      (exp(0.000871 * NO2_3h) - 1) +
                        (exp(0.000537 * O3_3h)  - 1) +
                        (exp(0.000487 * PM25_3h) - 1)
  )
  
  calib <- calib |>
    mutate(
      AQHI          = apply_aqhi_ceiling(AQHI_raw, PM25_1h),
      NO2_contrib   = (exp(0.000871 * NO2_3h)  - 1) / contrib_sum,
      O3_contrib    = (exp(0.000537 * O3_3h)   - 1) / contrib_sum,
      PM25_contrib  = (exp(0.000487 * PM25_3h) - 1) / contrib_sum
    ) |>
    # --- NEW: safe winner pick ------------------------------------------------
  mutate(
    Top_AQHI_Contributor = pmap_chr(
      list(NO2_contrib, O3_contrib, PM25_contrib),
      function(no2, o3, pm25) {
        vals <- c(NO2 = no2, O3 = o3, `PM2.5` = pm25)
        if (all(is.na(vals))) NA_character_            # ← avoid length-0
        else names(vals)[which.max(replace_na(vals, -Inf))]
      }
    )
  ) |>
    select(-ends_with("_3h"), -PM25_1h, -AQHI_raw)
  
  # ── (7) write output -------------------------------------------------------
  sensor_dir <- path(output_folder, sid)
  dir_create(sensor_dir)
  
  outfile <- path(
    sensor_dir,
    glue("{sid}_calibrated_{format(min(extract_date(last_two)), '%Y_%m_%d')}_to_{format(max(extract_date(last_two)), '%Y_%m_%d')}.csv")
  )
  readr::write_csv(calib, outfile, na = "")
  
  message("  ✔ wrote ", outfile)
}
