#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(dplyr); library(lubridate); library(randomForest); library(openair)
  library(readr); library(purrr); library(tidyr); library(fs); library(glue)
  library(stringr); library(gtools); library(zoo)
})
source("caps_core.R", local = TRUE)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript scripts/calibrate_data_bg.R <sensor_id> [UTC_DATE]")
sensor_id <- args[1]
utc_date  <- if (length(args) >= 2 && nzchar(args[2])) args[2] else format(Sys.time() - days(1), "%Y-%m-%d")

model_path <- Sys.getenv("CAL_MODEL_PATH",
                         unset = file.path("RAMP_Calibration_Models", sensor_id, "Calibration_Models.obj"))
data_dir   <- "data_bg"
out_dir    <- file.path("calibrated_data_bg", sensor_id); dir_create(out_dir)

start_utc <- as.POSIXct(paste0(utc_date, " 00:00:00"), tz="UTC")
end_utc   <- start_utc + hours(24)

# Load model
calibration_models <- local({
  env <- new.env(parent = emptyenv())
  ok  <- try(load(model_path, envir = env), silent = TRUE)
  if (!inherits(ok, "try-error") && exists("calibration_models", envir = env)) env$calibration_models else readRDS(model_path)
})

# load two most-recent raw days (typical filenames) — but we will filter by UTC window
all_files <- dir_ls(data_dir, recurse = TRUE, type = "file", glob = paste0(sensor_id, "_*.csv"))
if (!length(all_files)) stop("No raw files found for ", sensor_id)

# RAMP raw has DATE like "%m/%d/%y %H:%M:%S" UTC in your pipeline
read_select <- function(path) {
  read_csv(path, na = c("", "NA", "NaN"), guess_max = 100000, show_col_types = FALSE) |>
    select(any_of(c("DATE","CO","NO","NO2","O3","CO2","PM2.5","TE","RH"))) |>
    mutate(across(-DATE, as.character))
}

df <- bind_rows(lapply(all_files, read_select)) |>
  mutate(
    across(-DATE, readr::parse_double),
    date = as.POSIXct(DATE, format = "%m/%d/%y %H:%M:%S", tz = "UTC")
  ) |>
  filter(date >= start_utc, date < end_utc) |>
  select(-DATE)

if (nrow(df) == 0) stop("No rows fall within UTC day ", utc_date, " for sensor ", sensor_id)

df_15 <- timeAverage(df, avg.time = "15 min")

# Predictors
datetime   <- df_15$date
CO_RAMP    <- matrix(df_15$CO, ncol=1)
NO_RAMP    <- matrix(df_15$NO, ncol=1)
NO2_RAMP   <- matrix(df_15$NO2, ncol=1)
O3_RAMP    <- matrix(df_15$O3, ncol=1)
CO2_RAMP   <- matrix(df_15$CO2, ncol=1)
PM2_5_RAMP <- matrix(df_15$`PM2.5`, ncol=1)
T_RAMP     <- matrix(df_15$TE, ncol=1)
RH_RAMP    <- matrix(df_15$RH, ncol=1)
DP_RAMP <- 243.12 * (log(RH_RAMP/100) + (17.62*T_RAMP)/(243.12+T_RAMP)) /
  (17.62 - log(RH_RAMP/100) - (17.62*T_RAMP)/(243.12+T_RAMP))

data_gas <- cbind(CO_RAMP, NO_RAMP, NO2_RAMP, O3_RAMP, CO2_RAMP, T_RAMP, RH_RAMP)
data_pm  <- cbind(PM2_5_RAMP, T_RAMP, RH_RAMP, DP_RAMP)

mg <- calibration_models$gas$Hybrid
mp <- calibration_models$pm$Regression

CO_cal    <- CAPS_Hybrid_Apply(mg$CO, data_gas)
NO_cal    <- CAPS_Hybrid_Apply(mg$NO, data_gas)
NO2_cal   <- CAPS_Hybrid_Apply(mg$NO2, data_gas)
O3_cal    <- CAPS_Hybrid_Apply(mg$O3, data_gas)
CO2_cal   <- CAPS_Hybrid_Apply(mg$CO2, data_gas)
PM2_5_cal <- CAPS_PR_Apply(mp$PM2_5, data_pm)

# AQHI over the day (15-min cadence within the same 24h UTC window)
NO2_3h   <- zoo::rollapply(NO2_cal,   12, mean, fill = NA, align = "right", na.rm = TRUE)
O3_3h    <- zoo::rollapply(O3_cal,    12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_3h <- zoo::rollapply(PM2_5_cal, 12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_1h <- zoo::rollapply(PM2_5_cal,  4, mean, fill = NA, align = "right", na.rm = TRUE)

apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()

aqhi_val <- (10/10.4)*100*((exp(0.000871*NO2_3h)-1)+(exp(0.000537*O3_3h)-1)+(exp(0.000487*PM2_5_3h)-1))
aqhi_df <- tibble::tibble(AQHI = mapply(apply_aqhi_ceiling, aqhi_val, PM2_5_1h))

outfile <- file.path(out_dir, glue("{sensor_id}_{utc_date}_UTC.csv"))
calibrated <- tibble::tibble(
  DATETIME_UTC = format(datetime, "%Y-%m-%d %H:%M:%S"),
  CO = CO_cal, NO = NO_cal, NO2 = NO2_cal, O3 = O3_cal, CO2 = CO2_cal, `PM2.5` = PM2_5_cal
) |> dplyr::bind_cols(aqhi_df)

readr::write_csv(calibrated, outfile, na = "")
message("✔ [BG] Wrote ", outfile)
