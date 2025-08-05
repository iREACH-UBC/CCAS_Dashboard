# ────────────────────────────────────────────────────────────────
#  Calibrate raw imports using 2032's models
# ────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(dplyr);  library(lubridate);  library(randomForest); library(openair);
  library(readr); library(purrr); library(tidyr)
})

# ─---- CONFIG ----------------------------------------------------------------
model_path <- "C:/Users/hdignoes/Documents/VCH_Wildfire_Caliobrations/RAMP_Calibration_Models/2032/Calibration_Models.obj"

source("caps_core.R", local = TRUE) 
message("→ CAPS helpers loaded")

sensor_id <- "2032"  # Change or pass as argument
data_dir  <- "data"
out_dir <- file.path("calibrated_data/", sensor_id,"/")

# HELPER
apply_aqhi_ceiling <- function(aqhi_vec, pm25_1h_vec)
  pmax(round(aqhi_vec), ceiling(pm25_1h_vec / 10)) |> as.integer()
#-------------------------------------------------------------------------------


# Time window: past 48h from now (in pst) be very careful with timezones here
now_pst     <- with_tz(Sys.time(), "America/Los_Angeles")
past_48h    <- now_pst - hours(48)

# Get list of all candidate files
all_files <- dir_ls(data_dir, recurse = TRUE, type = "file", glob = "*.csv")

# Filter files that match the sensor_id and date pattern
pattern <- glue::glue("{sensor_id}_\\d{{4}}-\\d{{2}}-\\d{{2}}\\.csv")
matches <- all_files[str_detect(path_file(all_files), pattern)]

# Extract date from filename
extract_date <- function(path) {
  date_str <- str_match(path_file(path), "_(\\d{4}-\\d{2}-\\d{2})\\.csv")[,2]
  ymd(date_str)
}

files_tbl <- tibble(
  path = matches,
  date_file = extract_date(matches)
) %>%
  filter(!is.na(date_file)) %>%
  arrange(desc(date_file))

if (nrow(files_tbl) == 0) {
  stop("No matching files found for sensor ID ", sensor_id)
}
if (files_tbl$date_file[1] != as_date(now_pst)) {
  stop("No data for today found")
}
# ── LOAD CSVs ────────────────────────────────────────
today <- read.csv(files_tbl$path[1], stringsAsFactors = FALSE)
yesterday <- read.csv(files_tbl$path[2], stringsAsFactors = FALSE)

col_names <- c("DATE", "CO", "NO", "NO2", "O3", "CO2", "PM2.5", "TE", "RH")

today <- today %>% select(all_of(col_names))
yesterday <- yesterday %>% select(all_of(col_names))

df <- bind_rows(today, yesterday) #df now has the past 2ish days of data
df <- df %>% rename(date = DATE)
df$date <- as.POSIXct(df$date, format = "%m/%d/%y %H:%M:%S", tz = "UTC")
df_15min <- timeAverage(df, avg.time = "15 min")

datetime <- df_15min$date
CO_RAMP <- matrix(df_15min$CO, ncol=1)
NO_RAMP <- matrix(df_15min$NO, ncol=1)
NO2_RAMP <- matrix(df_15min$NO2, ncol=1)
O3_RAMP <- matrix(df_15min$O3, ncol=1)
CO2_RAMP <- matrix(df_15min$CO2, ncol=1)
PM2_5_RAMP <- matrix(df_15min$PM2.5, ncol=1)
T_RAMP <- matrix(df_15min$TE, ncol=1)
RH_RAMP <- matrix(df_15min$RH, ncol=1)
DP_RAMP <- 243.12 * (log(RH_RAMP / 100) + (17.62 * T_RAMP) / (243.12 + T_RAMP)) / (17.62 - log(RH_RAMP / 100) - (17.62 * T_RAMP) / (243.12 + T_RAMP))

data <- cbind(CO_RAMP, NO_RAMP, NO2_RAMP, O3_RAMP, CO2_RAMP, T_RAMP, RH_RAMP)
data_pm <- cbind(PM2_5_RAMP, T_RAMP, RH_RAMP, DP_RAMP)

# load model
cal_model <- load(model_path)
cal_model <- calibration_models$gas$Hybrid

# RUN CALIBRATIONS
CO_calibrated <- CAPS_Hybrid_Apply(cal_model$CO, data)
NO_calibrated <- CAPS_Hybrid_Apply(cal_model$NO, data)
NO2_calibrated <- CAPS_Hybrid_Apply(cal_model$NO2, data)
O3_calibrated <- CAPS_Hybrid_Apply(cal_model$O3, data)
CO2_calibrated <- CAPS_Hybrid_Apply(cal_model$CO2, data)
PM2_5_calibrated <- CAPS_PR_Apply(calibration_models$pm$Regression$PM2_5, data_pm)


#----------AQHI AND CONTRIBUTIONS--------------------------------------------

NO2_3h  = rollapply(NO2_calibrated,  12, mean, fill = NA, align = "right", na.rm = TRUE)
O3_3h   = rollapply(O3_calibrated,   12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_3h = rollapply(PM2_5_calibrated,12, mean, fill = NA, align = "right", na.rm = TRUE)
PM2_5_1h = rollapply(PM2_5_calibrated, 4, mean, fill = NA, align = "right", na.rm = TRUE)
AQHI = (10 / 10.4) * 100 * (
  (exp(0.000871 * NO2_3h) - 1) +
    (exp(0.000537 * O3_3h)  - 1) +
    (exp(0.000487 * PM25_3h) - 1)
)

df <- tibble(
  AQHI = (10 / 10.4) * 100 * (
    (exp(0.000871 * NO2_3h) - 1) +
      (exp(0.000537 * O3_3h) - 1) +
      (exp(0.000487 * PM2_5_3h) - 1)
  ),
  PM2_5_1h = PM2_5_1h,
  NO2_contrib_raw = (exp(0.000871 * NO2_3h) - 1),
  O3_contrib_raw  = (exp(0.000537 * O3_3h) - 1),
  PM2_5_contrib_raw = (exp(0.000487 * PM2_5_3h) - 1)
) |>
  mutate(
    AQHI = mapply(apply_aqhi_ceiling, AQHI, PM2_5_1h),
    contrib_sum = NO2_contrib_raw + O3_contrib_raw + PM2_5_contrib_raw,
    NO2_contrib = NO2_contrib_raw / contrib_sum,
    O3_contrib = O3_contrib_raw / contrib_sum,
    PM2_5_contrib = PM2_5_contrib_raw / contrib_sum,
    Top_AQHI_contrib = pmap_chr(
      list(NO2_contrib, O3_contrib, PM2_5_contrib),
      function(no2, o3, pm25) {
        vals <- c(NO2 = no2, O3 = o3, `PM2.5` = pm25)
        if (all(is.na(vals))) NA_character_
        else names(vals)[which.max(replace_na(vals, -Inf))]
      }
    )
  ) |>
  select(AQHI, NO2_contrib, O3_contrib, PM2_5_contrib, Top_AQHI_contrib)


# Join it all
calibrated_data <- cbind(datetime, CO_calibrated, NO_calibrated, NO2_calibrated, 
                         O3_calibrated, CO2_calibrated, PM2_5_calibrated,
                         AQHI, NO2_contrib, O3_contrib, PM2_5_contrib, 
                         Top_AQHI_contrib)

  colnames(calibrated_data) <- c(
  "DATE", "CO", "NO", "NO2", "O3", "CO2", "PM2.5", "AQHI",
  "NO2_contrib", "O3_contrib", "PM25_contrib", "Top_AQHI_Contributor"
  )

  calibrated_data <- as.data.frame(calibrated_data)
  calibrated_data <- calibrated_data %>%
    mutate(across(where(is.matrix), as.vector))
  calibrated_data$DATE <- as.POSIXct(calibrated_data$DATE, origin = "1970-01-01", tz = "UTC")
  
start_str <- format(past_48h, "%Y_%m_%d")
end_str   <- format(now_pst, "%Y_%m_%d")

outfile <- file.path(out_dir, paste0(sensor_id, "_calibrated_", start_str, "_to_", end_str, ".csv"))
write_csv(calibrated_data, outfile)
