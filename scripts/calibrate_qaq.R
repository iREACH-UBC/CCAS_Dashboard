#!/usr/bin/env Rscript
# ─────────────────────────────────────────────────────────────────────────────
#  Calibrate QuantAQ MOD-* data (QAQ format) using RAMP-style pipeline
#  - Expects columns: co, co2, no, no2, o3, pm1, pm10, pm25, temp, rh,
#                     timestamp_local (ISO8601), sn (sensor id)
#  - 48h window, 15-min timeAverage, explicit CAPS_* calls per pollutant
#  - Output: calibrated_data/<sid>/<sid>_calibrated_<start>_to_<end>.csv
# ─────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(dplyr); library(lubridate); library(readr)
  library(purrr); library(tidyr); library(fs); library(glue); library(stringr)
  library(zoo);  library(tibble)
})

# ── CONFIG -------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1 || is.na(args[1]) || args[1] == "") {
  stop("Usage: Rscript scripts/calibrate_qaq_rampish.R MOD-00616")
}
sensor_id <- args[1]

data_dir <- "data"
out_dir  <- file.path("calibrated_data", sensor_id); dir_create(out_dir)

# Model path: prefer env var, else per-sensor default (like RAMP script)
model_path <- Sys.getenv(
  "CAL_MODEL_PATH",
  unset = file.path("RAMP_Calibration_Models", sensor_id, "Calibration_Models.obj")
)

if (!file.exists(model_path)) stop("Model not found: ", model_path)

# Optional device clock offset (hours) to align with RAMPs if needed
clock_offset_hours <- suppressWarnings(as.numeric(Sys.getenv("CLOCK_OFFSET_HOURS", "0")))
if (is.na(clock_offset_hours)) clock_offset_hours <- 0

# ── Load CAPS models ----------------------------------------------------------
message("→ loading CAPS models …")
calibration_models <- local({
  env <- new.env(parent = emptyenv())
  ok  <- try(load(model_path, envir = env), silent = TRUE)
  if (!inherits(ok, "try-error")) {
    # Support both styles:
    if (exists("calibration_models", envir = env)) env$calibration_models
    else if (exists("Calibration_Models", envir = env)) env$Calibration_Models
    else {
      # Try RDS fallback
      readRDS(model_path)
    }
  } else {
    readRDS(model_path)
  }
})

# Helper to fetch a model regardless of object layout
get_model <- function(tree, group, key) {
  # Try RAMP-style: $gas$Hybrid$CO or $pm$Regression$PM2_5
  try1 <- try(tree[[group]][["Hybrid"]][[key]], silent = TRUE)
  if (!inherits(try1, "try-error") && !is.null(try1)) return(try1)
  try2 <- try(tree[[group]][["Regression"]][[key]], silent = TRUE)
  if (!inherits(try2, "try-error") && !is.null(try2)) return(try2)
  # Try flat Calibration_Models[["CO"]], etc.
  try3 <- try(tree[[key]], silent = TRUE)
  if (!inherits(try3, "try-error") && !is.null(try3)) return(try3)
  NULL
}

# ── AQHI helpers --------------------------------------------------------------
apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()

# ── Time window: past 48 h in PST/PDT ----------------------------------------
tz_local  <- "America/Los_Angeles"
now_pst   <- with_tz(Sys.time(), tz_local)
past_48h  <- now_pst - hours(48)

# ── File discovery: accept hyphen or underscore date separator ----------------
all_files <- dir_ls(data_dir, recurse = TRUE, type = "file", glob = "*.csv")

# Match MOD-00616-YYYY-MM-DD*.csv or MOD-00616_YYYY-MM-DD*.csv
pattern <- glue("^{sensor_id}[-_](\\d{{4}}-\\d{{2}}-\\d{{2}}).*\\.csv$")
matches <- all_files[str_detect(path_file(all_files), pattern)]

if (!length(matches)) stop("No QAQ files found for ", sensor_id)

extract_date <- function(p) {
  m <- str_match(path_file(p), "[-_](\\d{4}-\\d{2}-\\d{2})")
  if (is.na(m[1,2])) NA_Date_ else ymd(m[1,2])
}

files_tbl <- tibble(path = matches, date_file = map(matches, extract_date) |> unlist()) %>%
  filter(!is.na(date_file)) %>%
  arrange(desc(date_file))

if (nrow(files_tbl) < 1) stop("No dated QAQ files found for ", sensor_id)

# ── Reader that maps QAQ → working columns -----------------------------------
read_qaq <- function(path) {
  # Expect: co, co2, no, no2, o3, pm1, pm10, pm25, temp, rh, timestamp_local, sn
  df <- read_csv(path, show_col_types = FALSE, na = c("", "NA", "NaN")) %>%
    # Filter this sensor only (defensive)
    filter(is.na(sn) | sn == sensor_id) %>%
    transmute(
      DATE = ymd_hms(timestamp_local, tz = tz_local, quiet = TRUE) + hours(clock_offset_hours),
      CO   = as.numeric(co),
      NO   = as.numeric(no),
      NO2  = as.numeric(no2),
      O3   = as.numeric(o3),
      CO2  = as.numeric(co2),
      `PM1.0` = as.numeric(pm1),
      PM10 = as.numeric(pm10),
      `PM2.5` = as.numeric(pm25),
      TE   = as.numeric(temp),   # RAMP uses TE for temperature (°C)
      RH   = as.numeric(rh)      # (%)
    ) %>%
    arrange(DATE) %>%
    filter(!is.na(DATE))
  df
}

# Read newest two days (or more) until we cover past_48h
parts <- list(); earliest <- now_pst
for (p in files_tbl$path) {
  message("  • reading ", path_file(p))
  raw <- tryCatch(read_qaq(p), error = function(e) { warning(conditionMessage(e)); NULL })
  if (is.null(raw) || nrow(raw) == 0) next
  parts <- append(parts, list(raw))
  earliest <- min(earliest, min(raw$DATE, na.rm = TRUE))
  if (earliest <= past_48h) break
}
if (!length(parts)) stop("No usable rows after parsing for ", sensor_id)

raw_all <- bind_rows(parts) %>%
  filter(DATE >= past_48h) %>%
  distinct(DATE, .keep_all = TRUE)

# ── 15-min averaging (RAMP-style with openair) --------------------------------
df_avg <- raw_all %>%
  mutate(date = DATE) %>%
  select(date, CO, NO, NO2, O3, CO2, `PM2.5`, `PM1.0`, PM10, TE, RH)

df_15 <- raw_all %>%
  transmute(
    date = DATE,
    CO, NO, NO2, O3, CO2, `PM2.5`, `PM1.0`, PM10, TE, RH
  ) %>%
  mutate(bin = lubridate::floor_date(date, "15 minutes")) %>%
  group_by(bin) %>%
  summarise(
    CO   = mean(CO,   na.rm = TRUE),
    NO   = mean(NO,   na.rm = TRUE),
    NO2  = mean(NO2,  na.rm = TRUE),
    O3   = mean(O3,   na.rm = TRUE),
    CO2  = mean(CO2,  na.rm = TRUE),
    `PM2.5` = mean(`PM2.5`, na.rm = TRUE),
    `PM1.0` = mean(`PM1.0`, na.rm = TRUE),
    PM10 = mean(PM10, na.rm = TRUE),
    TE   = mean(TE,   na.rm = TRUE),
    RH   = mean(RH,   na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(date = bin)

# ── Build predictor matrices (RAMP-style) -------------------------------------
datetime   <- df_15$date
CO_RAMP    <- matrix(df_15$CO, ncol = 1)
NO_RAMP    <- matrix(df_15$NO, ncol = 1)
NO2_RAMP   <- matrix(df_15$NO2, ncol = 1)
O3_RAMP    <- matrix(df_15$O3, ncol = 1)
CO2_RAMP   <- matrix(df_15$CO2, ncol = 1)
PM2_5_RAMP <- matrix(df_15$`PM2.5`, ncol = 1)
T_RAMP     <- matrix(df_15$TE, ncol = 1)
RH_RAMP    <- matrix(df_15$RH, ncol = 1)

# Dew point (°C) from T (°C) and RH (%)
DP_RAMP <- 243.12 * (log(RH_RAMP / 100) + (17.62 * T_RAMP) / (243.12 + T_RAMP)) /
  (17.62 - log(RH_RAMP / 100) - (17.62 * T_RAMP) / (243.12 + T_RAMP))

data_gas <- cbind(CO_RAMP, NO_RAMP, NO2_RAMP, O3_RAMP, CO2_RAMP, T_RAMP, RH_RAMP)
data_pm  <- cbind(PM2_5_RAMP, T_RAMP, RH_RAMP, DP_RAMP)

# ── Apply calibrations (explicit, with graceful fallback) ---------------------
# Prefer Hybrid for gases, Regression for PM2.5 if available
m_CO   <- get_model(calibration_models, "gas", "CO")
m_NO   <- get_model(calibration_models, "gas", "NO")
m_NO2  <- get_model(calibration_models, "gas", "NO2")
m_O3   <- get_model(calibration_models, "gas", "O3")
m_CO2  <- get_model(calibration_models, "gas", "CO2")
m_PM25 <- get_model(calibration_models, "pm",  "PM2_5")

apply_safe <- function(fun, ...) {
  tryCatch(fun(...), error = function(e) { warning(conditionMessage(e)); NULL })
}

# If CAPS_* not present, pass through raw
cap_or_raw <- function(model, x_raw, x_mat, fun) {
  if (is.null(model) || !exists(fun, mode = "function")) return(as.numeric(x_raw))
  res <- apply_safe(get(fun), model, x_mat)
  if (is.null(res)) as.numeric(x_raw) else as.numeric(res)
}

CO_cal    <- cap_or_raw(m_CO,   df_15$CO,    data_gas, "CAPS_Hybrid_Apply")
NO_cal    <- cap_or_raw(m_NO,   df_15$NO,    data_gas, "CAPS_Hybrid_Apply")
NO2_cal   <- cap_or_raw(m_NO2,  df_15$NO2,   data_gas, "CAPS_Hybrid_Apply")
O3_cal    <- cap_or_raw(m_O3,   df_15$O3,    data_gas, "CAPS_Hybrid_Apply")
CO2_cal   <- cap_or_raw(m_CO2,  df_15$CO2,   data_gas, "CAPS_Hybrid_Apply")
PM2_5_cal <- {
  # Do regression, but Hybrid if only that exists
  if (!is.null(m_PM25) && exists("CAPS_PR_Apply", mode = "function")) {
    res <- apply_safe(CAPS_PR_Apply, m_PM25, data_pm)
    if (!is.null(res)) as.numeric(res) else as.numeric(df_15$`PM2.5`)
  } else if (!is.null(m_PM25) && exists("CAPS_Hybrid_Apply", mode = "function")) {
    res <- apply_safe(CAPS_Hybrid_Apply, m_PM25, data_pm)
    if (!is.null(res)) as.numeric(res) else as.numeric(df_15$`PM2.5`)
  } else {
    as.numeric(df_15$`PM2.5`)
  }
}

# ── AQHI (RAMP-style: counts for 15-min cadence) ------------------------------
NO2_3h   <- rollapply(NO2_cal,   12, mean, fill = NA, align = "right", na.rm = TRUE)
O3_3h    <- rollapply(O3_cal,    12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_3h <- rollapply(PM2_5_cal, 12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_1h <- rollapply(PM2_5_cal,  4, mean, fill = NA, align = "right", na.rm = TRUE)

aqhi_val <- (10 / 10.4) * 100 * (
  (exp(0.000871 * NO2_3h) - 1) +
    (exp(0.000537 * O3_3h)  - 1) +
    (exp(0.000487 * PM2_5_3h) - 1)
)

aqhi_df <- tibble(
  AQHI = mapply(apply_aqhi_ceiling, aqhi_val, PM2_5_1h),
  NO2_contrib_raw   = (exp(0.000871 * NO2_3h) - 1),
  O3_contrib_raw    = (exp(0.000537 * O3_3h)  - 1),
  PM2_5_contrib_raw = (exp(0.000487 * PM2_5_3h) - 1)
) %>%
  mutate(
    contrib_sum = NO2_contrib_raw + O3_contrib_raw + PM2_5_contrib_raw,
    NO2_contrib = NO2_contrib_raw   / contrib_sum,
    O3_contrib  = O3_contrib_raw    / contrib_sum,
    PM2_5_contrib = PM2_5_contrib_raw / contrib_sum,
    Top_AQHI_Contributor = pmap_chr(
      list(NO2_contrib, O3_contrib, PM2_5_contrib),
      function(no2, o3, pm25) {
        vals <- c(NO2 = no2, O3 = o3, `PM2.5` = pm25)
        if (all(is.na(vals))) NA_character_
        else names(vals)[which.max(replace_na(vals, -Inf))]
      }
    )
  ) %>%
  select(AQHI, NO2_contrib, O3_contrib, PM2_5_contrib, Top_AQHI_Contributor)

# ── Final output ---------------------------------------------------------------
calibrated <- tibble(
  DATE   = datetime,           # POSIXct
  CO     = CO_cal/1000,
  NO     = NO_cal,
  NO2    = NO2_cal,
  O3     = O3_cal,
  CO2    = CO2_cal,
  `PM2.5`= PM2_5_cal
) %>%
  bind_cols(aqhi_df) %>%
  mutate(DATE = format(with_tz(DATE, tz_local), "%Y-%m-%d %H:%M:%S"))

start_str <- format(past_48h, "%Y_%m_%d")
end_str   <- format(now_pst,  "%Y_%m_%d")
outfile   <- file.path(out_dir, glue("{sensor_id}_calibrated_{start_str}_to_{end_str}.csv"))

write_csv(calibrated, outfile, na = "")
message("✔ Calibrated data written to ", outfile, " (", nrow(calibrated), " rows)")
