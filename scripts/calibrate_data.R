# ────────────────────────────────────────────────────────────────
#  Calibrate raw imports using CAPS model for any sensor
# ────────────────────────────────────────────────────────────────
suppressPackageStartupMessages({
  library(dplyr); library(lubridate); library(randomForest); library(openair)
  library(readr); library(purrr); library(tidyr); library(fs); library(glue); 
  library(stringr); library(gtools); library(zoo)

})

source("caps_core.R", local = TRUE)
message("→ CAPS helpers loaded")

# ── ARGS ─────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript calibrate_caps_sensor.R <sensor_id>")
sensor_id <- args[1]

model_path <- Sys.getenv("CAL_MODEL_PATH", unset = file.path("RAMP_Calibration_Models", sensor_id, "Calibration_Models.obj"))
data_dir   <- "data"
out_dir    <- file.path("calibrated_data", sensor_id)
dir_create(out_dir)

# ── Load calibration model ───────────────────────────────────────
message("→ loading CAPS models into memory …")
calibration_models <- local({
  env <- new.env(parent = emptyenv())
  ok  <- try(load(model_path, envir = env), silent = TRUE)
  if (!inherits(ok, "try-error") && exists("calibration_models", envir = env))
    env$calibration_models
  else
    readRDS(model_path)
})

# ── Helper ───────────────────────────────────────────────────────
apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()

# ── Time window: past 48h from now (PST) ─────────────────────────
now_pst  <- with_tz(Sys.time(), "America/Los_Angeles")
past_48h <- now_pst - hours(48)

# ── Get candidate files ──────────────────────────────────────────
all_files <- dir_ls(data_dir, recurse = TRUE, type = "file", glob = "*.csv")
pattern   <- glue("{sensor_id}_\\d{{4}}-\\d{{2}}-\\d{{2}}\\.csv")
matches   <- all_files[str_detect(path_file(all_files), pattern)]

extract_date <- function(path) {
  ymd(str_match(path_file(path), "_(\\d{4}-\\d{2}-\\d{2})\\.csv")[, 2])
}

files_tbl <- tibble(path = matches, date_file = extract_date(matches)) %>%
  filter(!is.na(date_file)) %>%
  arrange(desc(date_file))

if (nrow(files_tbl) < 2)
  stop("Need at least 2 days of data for sensor ", sensor_id)


# ── Load and combine ─────────────────────────────────────────────
keep_cols <- c("DATE", "CO", "NO", "NO2", "O3", "CO2", "PM2.5", "TE", "RH")

read_select <- function(path) {
  readr::read_csv(path, na = c("", "NA", "NaN"),
                  guess_max = 100000, show_col_types = FALSE) |>
    dplyr::select(dplyr::any_of(keep_cols)) |>              # ignore weird extras like `-9`, `0...27`
    dplyr::mutate(dplyr::across(-DATE, ~ as.character(.x))) # unify to character pre-bind
}

today     <- read_select(files_tbl$path[1])
yesterday <- read_select(files_tbl$path[2])

df <- dplyr::bind_rows(today, yesterday) |>
  dplyr::mutate(
    dplyr::across(-DATE, ~ readr::parse_double(.x)),        # consistent numeric after bind
    date = as.POSIXct(DATE, format = "%m/%d/%y %H:%M:%S", tz = "UTC")
  ) |>
  dplyr::select(-DATE)
df_15min <- timeAverage(df, avg.time = "15 min")

# ── Build predictor matrices ─────────────────────────────────────
datetime <- df_15min$date
CO_RAMP  <- matrix(df_15min$CO, ncol = 1)
NO_RAMP  <- matrix(df_15min$NO, ncol = 1)
NO2_RAMP <- matrix(df_15min$NO2, ncol = 1)
O3_RAMP  <- matrix(df_15min$O3, ncol = 1)
CO2_RAMP <- matrix(df_15min$CO2, ncol = 1)
PM2_5_RAMP <- matrix(df_15min$`PM2.5`, ncol = 1)
T_RAMP <- matrix(df_15min$TE, ncol = 1)
RH_RAMP <- matrix(df_15min$RH, ncol = 1)

DP_RAMP <- 243.12 * (log(RH_RAMP / 100) + (17.62 * T_RAMP) / (243.12 + T_RAMP)) / 
  (17.62 - log(RH_RAMP / 100) - (17.62 * T_RAMP) / (243.12 + T_RAMP))

data     <- cbind(CO_RAMP, NO_RAMP, NO2_RAMP, O3_RAMP, CO2_RAMP, T_RAMP, RH_RAMP)
data_pm  <- cbind(PM2_5_RAMP, T_RAMP, RH_RAMP, DP_RAMP)

# ── Apply calibrations ───────────────────────────────────────────
model_gas <- calibration_models$gas$Hybrid
model_pm  <- calibration_models$pm$Regression

CO_cal    <- CAPS_Hybrid_Apply(model_gas$CO, data)
NO_cal    <- CAPS_Hybrid_Apply(model_gas$NO, data)
NO2_cal   <- CAPS_Hybrid_Apply(model_gas$NO2, data)
O3_cal    <- CAPS_Hybrid_Apply(model_gas$O3, data)
CO2_cal   <- CAPS_Hybrid_Apply(model_gas$CO2, data)
PM2_5_cal <- CAPS_PR_Apply(model_pm$PM2_5, data_pm)

# ── AQHI computation ─────────────────────────────────────────────
NO2_3h   <- rollapply(NO2_cal,   12, mean, fill = NA, align = "right", na.rm = TRUE)
O3_3h    <- rollapply(O3_cal,    12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_3h <- rollapply(PM2_5_cal, 12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_1h <- rollapply(PM2_5_cal,  4, mean, fill = NA, align = "right", na.rm = TRUE)

aqhi_val <- (10 / 10.4) * 100 * (
  (exp(0.000871 * NO2_3h) - 1) +
    (exp(0.000537 * O3_3h) - 1) +
    (exp(0.000487 * PM2_5_3h) - 1)
)

aqhi_df <- tibble(
  AQHI = mapply(apply_aqhi_ceiling, aqhi_val, PM2_5_1h),
  NO2_contrib_raw = (exp(0.000871 * NO2_3h) - 1),
  O3_contrib_raw  = (exp(0.000537 * O3_3h) - 1),
  PM2_5_contrib_raw = (exp(0.000487 * PM2_5_3h) - 1)
) %>%
  mutate(
    contrib_sum = NO2_contrib_raw + O3_contrib_raw + PM2_5_contrib_raw,
    NO2_contrib = NO2_contrib_raw / contrib_sum,
    O3_contrib  = O3_contrib_raw  / contrib_sum,
    PM2_5_contrib = PM2_5_contrib_raw / contrib_sum,
    Top_AQHI_contributor = pmap_chr(
      list(NO2_contrib, O3_contrib, PM2_5_contrib),
      function(no2, o3, pm25) {
        vals <- c(NO2 = no2, O3 = o3, `PM2.5` = pm25)
        if (all(is.na(vals))) NA_character_
        else names(vals)[which.max(replace_na(vals, -Inf))]
      }
    )
  ) %>%
  select(AQHI, NO2_contrib, O3_contrib, PM2_5_contrib, Top_AQHI_contributor)

# ── Final output ─────────────────────────────────────────────────
calibrated_data <- as_tibble(cbind(
  DATE = datetime,
  CO = CO_cal, NO = NO_cal, NO2 = NO2_cal, O3 = O3_cal, CO2 = CO2_cal,
  `PM2.5` = PM2_5_cal,
  aqhi_df
)) %>%
  mutate(across(where(is.matrix), as.vector)) %>%
  mutate(DATE = format(with_tz(DATE, "America/Los_Angeles"), "%Y-%m-%d %H:%M:%S"))

start_str <- format(past_48h, "%Y_%m_%d")
end_str   <- format(now_pst, "%Y_%m_%d")
outfile   <- file.path(out_dir, glue("{sensor_id}_calibrated_{start_str}_to_{end_str}.csv"))

write_csv(calibrated_data, outfile, na = "")
message("✔ Calibrated data written to ", outfile)
