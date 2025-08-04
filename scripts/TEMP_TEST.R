#!/usr/bin/env Rscript
# ─── calibrate_data_local.R ────────────────────────────────────────────────
# Local-only edition: reads raw logger CSVs and *local* CAPS models,
# writes a strict 24 h PST window per sensor under calibrated_data_local/.

suppressPackageStartupMessages({
  library(dplyr);  library(readr);  library(lubridate);  library(stringr)
  library(purrr);  library(tibble); library(fs);          library(zoo)
  library(glue);   library(gtools); library(tidyr)
})

source("scripts/apply_caps_calibration.R")

# ── CONFIG --------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)

sensor_ids <- if (length(args)) args else c(
  "2021","2022","2040","2032","2042","2043","2024","2030"
)

data_folder        <- "data"                       # raw logger exports
output_folder      <- "calibrated_data_local"      # keep local artefacts apart
dir_create(output_folder)

# <<< LOCAL MODELS ONLY –- point to your .obj roots here >>>
cal_models_root <- "C:/Users/hdignoes/Documents/VCH_Wildfire_Caliobrations/RAMP_Calibration_Models"

# ── helpers -------------------------------------------------------------------
extract_date <- function(paths)
  as.Date(str_extract(path_file(paths), "\\d{4}-\\d{2}-\\d{2}"))

apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()

# ── time window ---------------------------------------------------------------
now_pst  <- with_tz(Sys.time(), "America/Los_Angeles")
past_24h <- now_pst - hours(24)          # strict 24 h window in PST

# ── MAIN LOOP -----------------------------------------------------------------
for (sid in sensor_ids) {
  message("── Sensor ", sid, " ────────────────────────────────────────────")
  
  # -- per-sensor model path ---------------------------------------------------
  model_file <- path(cal_models_root, sid, "Calibration_Models.obj")
  if (!file_exists(model_file)) {
    warning("  ✖ no model file for ", sid, " → ", model_file)
    next
  }
  
  # -- discover raw files (yesterday + today, PST) -----------------------------
  date_window <- seq.Date(as_date(now_pst) - 1, as_date(now_pst), by = "day")
  target_files <- glue("{sid}_{format(date_window, '%Y-%m-%d')}.csv")
  
  all_paths  <- dir_ls(data_folder, recurse = TRUE, type = "file")
  files_raw  <- all_paths[tolower(path_file(all_paths)) %in% tolower(target_files)]
  
  if (!length(files_raw)) {
    warning("  ✖ no raw data for ", sid)
    next
  }
  
  # -- calibrate until ≥ 24 h span --------------------------------------------
  calib_parts <- list(); earliest_ts <- now_pst
  for (p in files_raw[order(files_raw, decreasing = TRUE)]) {
    df_part <- apply_caps_calibration(sensor_id = sid,
                                      data_file  = p,
                                      model_path = model_file)
    calib_parts <- append(calib_parts, list(df_part))
    earliest_ts <- min(earliest_ts, min(df_part$date, na.rm = TRUE))
    if (earliest_ts <= past_24h) break
  }
  calibrated <- bind_rows(calib_parts)
  if (!nrow(calibrated)) next
  
  # -- tidy + convert to PST ---------------------------------------------------
  calib <- calibrated |>
    mutate(
      DATE = with_tz(date, "America/Los_Angeles") + hours(2),  # device clock +2 h
      .keep = "unused"
    ) |>
    rename(PM2.5 = PM2_5) |>
    select(DATE, CO, NO, NO2, O3, CO2, PM2.5, everything()) |>
    filter(DATE >= past_24h)
  
  # -- rolling means, AQHI, contributors --------------------------------------
  calib <- calib |>
    arrange(DATE) |>
    mutate(
      NO2_3h  = rollapply(NO2,  12, mean, fill = NA, align = "right", na.rm = TRUE),
      O3_3h   = rollapply(O3,   12, mean, fill = NA, align = "right", na.rm = TRUE),
      PM25_3h = rollapply(PM2.5,12, mean, fill = NA, align = "right", na.rm = TRUE),
      PM25_1h = rollapply(PM2.5, 4, mean, fill = NA, align = "right", na.rm = TRUE),
      AQHI_raw = (10/10.4) * 100 * (
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
      NO2_contrib  = (exp(0.000871 * NO2_3h) - 1) / contrib_sum,
      O3_contrib   = (exp(0.000537 * O3_3h)  - 1) / contrib_sum,
      PM25_contrib = (exp(0.000487 * PM25_3h) - 1) / contrib_sum,
      Top_AQHI_Contributor = pmap_chr(
        list(NO2_contrib, O3_contrib, PM25_contrib),
        ~ {
          vals <- c(NO2 = ..1, O3 = ..2, `PM2.5` = ..3)
          if (all(is.na(vals))) NA_character_
          else names(vals)[which.max(replace_na(vals, -Inf))]
        }
      )
    ) |>
    select(-ends_with("_3h"), -PM25_1h, -AQHI_raw)
  
  # -- write CSV (no Git sentinel) --------------------------------------------
  sensor_dir <- path(output_folder, sid); dir_create(sensor_dir)
  outfile <- path(
    sensor_dir,
    glue("{sid}_calibrated_{format(Sys.Date(), '%Y_%m_%d')}.csv")
  )
  write_csv(calib, outfile, na = "")
  message("  ✔ wrote ", outfile, " (", nrow(calib), " rows)")
}

message("\nDone – all sensors processed locally.")
