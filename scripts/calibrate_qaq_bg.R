#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(dplyr); library(lubridate); library(readr)
  library(purrr); library(tidyr); library(fs); library(glue); library(stringr)
  library(zoo);  library(tibble)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript scripts/calibrate_qaq_bg.R <sensor_id> [UTC_DATE]")
sensor_id <- args[1]
utc_date  <- if (length(args) >= 2 && nzchar(args[2])) args[2] else format(Sys.time() - days(1), "%Y-%m-%d")

data_dir <- "data_bg"
out_dir  <- file.path("calibrated_data_bg", sensor_id); dir_create(out_dir)

model_path <- Sys.getenv("CAL_MODEL_PATH",
                         unset = file.path("RAMP_Calibration_Models", sensor_id, "Calibration_Models.obj"))
if (!file.exists(model_path)) stop("Model not found: ", model_path)

# Load models (identical logic)
calibration_models <- local({
  env <- new.env(parent = emptyenv())
  ok  <- try(load(model_path, envir = env), silent = TRUE)
  if (!inherits(ok, "try-error")) {
    if (exists("calibration_models", envir = env)) env$calibration_models
    else if (exists("Calibration_Models", envir = env)) env$Calibration_Models
    else readRDS(model_path)
  } else readRDS(model_path)
})

# Time window (UTC)
start_utc <- as.POSIXct(paste0(utc_date, " 00:00:00"), tz="UTC")
end_utc   <- start_utc + hours(24)

# Read raw QAQ; prefer UTC timestamp if available; else convert local->UTC
TZ_LOCAL <- Sys.getenv("TZ_LOCAL", "America/Los_Angeles")

read_qaq_one <- function(path) {
  df <- read_csv(path, show_col_types = FALSE, na = c("", "NA", "NaN"))
  # normalize timestamp column
  if ("timestamp" %in% names(df)) {
    dt <- ymd_hms(df$timestamp, tz = "UTC", quiet = TRUE)
  } else if ("timestamp_local" %in% names(df)) {
    dt_local <- ymd_hms(df$timestamp_local, tz = TZ_LOCAL, quiet = TRUE)
    dt <- with_tz(dt_local, "UTC")
  } else {
    stop("No timestamp or timestamp_local column in: ", path)
  }
  
  df %>%
    mutate(DATETIME_UTC = dt) %>%
    filter(is.na(sn) | sn == sensor_id) %>%
    transmute(
      DATETIME_UTC,
      CO    = as.numeric(co),
      NO    = as.numeric(no),
      NO2   = as.numeric(no2),
      O3    = as.numeric(o3),
      CO2   = as.numeric(co2),
      `PM1.0` = suppressWarnings(as.numeric(pm1)),
      PM10  = suppressWarnings(as.numeric(pm10)),
      `PM2.5`= suppressWarnings(as.numeric(pm25)),
      TE    = as.numeric(temp),
      RH    = as.numeric(rh)
    ) %>%
    arrange(DATETIME_UTC) %>%
    filter(!is.na(DATETIME_UTC))
}

files <- dir_ls(data_dir, recurse = TRUE, type = "file",
                glob = glue("{sensor_id}*.csv"))
if (!length(files)) stop("No QAQ files for ", sensor_id)

raw <- purrr::map_dfr(files, ~tryCatch(read_qaq_one(.x), error = function(e) tibble()))
day  <- raw %>% filter(DATETIME_UTC >= start_utc, DATETIME_UTC < end_utc)
if (nrow(day) == 0) stop("No rows in UTC day ", utc_date, " for ", sensor_id)

# 15-min aggregation in UTC
df_15 <- day %>%
  mutate(bin = floor_date(DATETIME_UTC, "15 minutes")) %>%
  group_by(bin) %>%
  summarise(
    CO   = mean(CO,   na.rm = TRUE), NO = mean(NO, na.rm = TRUE),
    NO2  = mean(NO2,  na.rm = TRUE), O3 = mean(O3, na.rm = TRUE),
    CO2  = mean(CO2,  na.rm = TRUE),
    `PM2.5` = mean(`PM2.5`, na.rm = TRUE),
    `PM1.0` = mean(`PM1.0`, na.rm = TRUE), PM10 = mean(PM10, na.rm = TRUE),
    TE = mean(TE, na.rm = TRUE), RH = mean(RH, na.rm = TRUE),
    .groups = "drop"
  ) %>% rename(DATETIME_UTC = bin)

# predictors
datetime   <- df_15$DATETIME_UTC
CO_RAMP    <- matrix(df_15$CO, ncol=1)
NO_RAMP    <- matrix(df_15$NO, ncol=1)
NO2_RAMP   <- matrix(df_15$NO2, ncol=1)
O3_RAMP    <- matrix(df_15$O3, ncol=1)
CO2_RAMP   <- matrix(df_15$CO2, ncol=1)
PM2_5_RAMP <- matrix(df_15$`PM2.5`, ncol=1)
T_RAMP     <- matrix(df_15$TE, ncol=1)
RH_RAMP    <- matrix(df_15$RH, ncol=1)
DP_RAMP    <- 243.12 * (log(RH_RAMP/100) + (17.62*T_RAMP)/(243.12+T_RAMP)) /
  (17.62 - log(RH_RAMP/100) - (17.62*T_RAMP)/(243.12+T_RAMP))

data_gas <- cbind(CO_RAMP, NO_RAMP, NO2_RAMP, O3_RAMP, CO2_RAMP, T_RAMP, RH_RAMP)
data_pm  <- cbind(PM2_5_RAMP, T_RAMP, RH_RAMP, DP_RAMP)

get_model <- function(tree, group, key) {
  try1 <- try(tree[[group]][["Hybrid"]][[key]], silent = TRUE)
  if (!inherits(try1, "try-error") && !is.null(try1)) return(try1)
  try2 <- try(tree[[group]][["Regression"]][[key]], silent = TRUE)
  if (!inherits(try2, "try-error") && !is.null(try2)) return(try2)
  try3 <- try(tree[[key]], silent = TRUE)
  if (!inherits(try3, "try-error") && !is.null(try3)) return(try3)
  NULL
}

m_CO   <- get_model(calibration_models, "gas", "CO")
m_NO   <- get_model(calibration_models, "gas", "NO")
m_NO2  <- get_model(calibration_models, "gas", "NO2")
m_O3   <- get_model(calibration_models, "gas", "O3")
m_CO2  <- get_model(calibration_models, "gas", "CO2")
m_PM25 <- get_model(calibration_models, "pm",  "PM2_5")

apply_safe <- function(fun, ...) { tryCatch(fun(...), error=function(e){warning(e$message); NULL}) }
cap_or_raw <- function(model, x_raw, x_mat, fun) {
  if (is.null(model) || !exists(fun, mode="function")) return(as.numeric(x_raw))
  res <- apply_safe(get(fun), model, x_mat)
  if (is.null(res)) as.numeric(x_raw) else as.numeric(res)
}

CO_cal    <- cap_or_raw(m_CO,   df_15$CO,    data_gas, "CAPS_Hybrid_Apply")
NO_cal    <- cap_or_raw(m_NO,   df_15$NO,    data_gas, "CAPS_Hybrid_Apply")
NO2_cal   <- cap_or_raw(m_NO2,  df_15$NO2,   data_gas, "CAPS_Hybrid_Apply")
O3_cal    <- cap_or_raw(m_O3,   df_15$O3,    data_gas, "CAPS_Hybrid_Apply")
CO2_cal   <- cap_or_raw(m_CO2,  df_15$CO2,   data_gas, "CAPS_Hybrid_Apply")
PM2_5_cal <- {
  if (!is.null(m_PM25) && exists("CAPS_PR_Apply", mode="function")) {
    res <- apply_safe(CAPS_PR_Apply, m_PM25, data_pm)
    if (!is.null(res)) as.numeric(res) else as.numeric(df_15$`PM2.5`)
  } else if (!is.null(m_PM25) && exists("CAPS_Hybrid_Apply", mode="function")) {
    res <- apply_safe(CAPS_Hybrid_Apply, m_PM25, data_pm)
    if (!is.null(res)) as.numeric(res) else as.numeric(df_15$`PM2.5`)
  } else as.numeric(df_15$`PM2.5`)
}

NO2_3h   <- rollapply(NO2_cal,   12, mean, fill = NA, align = "right", na.rm = TRUE)
O3_3h    <- rollapply(O3_cal,    12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_3h <- rollapply(PM2_5_cal, 12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_1h <- rollapply(PM2_5_cal,  4, mean, fill = NA, align = "right", na.rm = TRUE)

apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()

aqhi_val <- (10/10.4)*100*((exp(0.000871*NO2_3h)-1)+(exp(0.000537*O3_3h)-1)+(exp(0.000487*PM2_5_3h)-1))
aqhi_df <- tibble(AQHI = mapply(apply_aqhi_ceiling, aqhi_val, PM2_5_1h))

outfile <- file.path(out_dir, glue("{sensor_id}_{utc_date}_UTC.csv"))
calibrated <- tibble(
  DATETIME_UTC = format(datetime, "%Y-%m-%d %H:%M:%S"),
  CO = CO_cal, NO = NO_cal, NO2 = NO2_cal, O3 = O3_cal, CO2 = CO2_cal, `PM2.5` = PM2_5_cal
) %>% bind_cols(aqhi_df)

write_csv(calibrated, outfile, na = "")
message("✔ [BG] Wrote ", outfile)
