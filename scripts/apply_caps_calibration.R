# ── apply_caps_calibration.R ─────────────────────────────────────────────
apply_caps_calibration <- function(sensor_id,
                                   data_file,
                                   bucket   = "outdoor-calibrations",
                                   tz_raw   = "Etc/GMT-8",
                                   avg_time = "15 min",
                                   out_dir  = NULL) {
  
  ## 1 ── Download model OBJ from R2 ----------------------------------------
  library(aws.s3); library(glue)
  
  host <- sub("^https?://", "", Sys.getenv("R2_ENDPOINT"))
  if (host == "") stop("R2_ENDPOINT not set")
  
  key <- glue("{sensor_id}/Calibration_Models.obj")
  message(glue("→ Downloading {sensor_id} model from R2 …"))
  
  bytes <- aws.s3::get_object(
    object   = key,
    bucket   = bucket,
    base_url = host,
    region   = ""
  )
  
  ## 2 ── Peek header safely (works for gzip & plain) ------------------------
  is_gz <- identical(rawToChar(bytes[1:3]), "\x1f\x8b\b")
  
  peek_con <- rawConnection(bytes, open = "rb")
  if (is_gz) peek_con <- gzcon(peek_con)   # transparently decompress stream
  magic <- rawToChar(readBin(peek_con, "raw", 4))
  close(peek_con)
  
  if (!magic %in% c("RDX2", "RDX3"))
    stop(glue("File '{key}' is not a valid RData/OBJ (header = '{magic}')."))
  
  ## 3 ── Load calibration_models from OBJ ----------------------------------
  load_con <- rawConnection(bytes, open = "rb")
  if (is_gz) load_con <- gzcon(load_con)
  
  tmp_env <- new.env()
  load(load_con, envir = tmp_env)
  close(load_con)
  
  if (!exists("calibration_models", envir = tmp_env, inherits = FALSE))
    stop(glue("Object 'calibration_models' not found in '{key}'."))
  
  calibration_models <- tmp_env$calibration_models
  
  ## 4 ── Libraries for downstream work -------------------------------------
  suppressPackageStartupMessages({
    library(dplyr);  library(readr);  library(lubridate); library(tibble)
    library(purrr);   library(tidyr); library(openair);   library(zoo)
    library(fs);      library(gtools); library(randomForest)
  })
  
  message("→ Loading CAPS helpers and models …")
  source("caps_core.R", local = TRUE)        # creates CAPS_* helpers
  
  ## 5 ── Read & tidy raw logger file ---------------------------------------
  raw <- read_csv(data_file, col_names = FALSE, show_col_types = FALSE)
  names(raw) <- paste0("V", seq_along(raw))
  
  raw <- raw |>
    select(V1, V3, V4, V5, V6, V7, V8, V9, V11) |>
    rlang::set_names(c("date","CO_RAMP","NO_RAMP","NO2_RAMP","O3_RAMP",
                       "CO2_RAMP","T_RAMP","RH_RAMP","PM_RAMP")) |>
    mutate(
      date  = parse_date_time(date, orders = "%m/%d/%y %H:%M:%S"),
      across(-date, parse_number)
    ) |>
    filter(!is.na(date))
  
  raw$date <- force_tz(raw$date, tz_raw)
  
  ## daylight-saving shim ----------------------------------------------------
  raw <- dplyr::bind_rows(
    filter(raw, date < "2025-03-09 02:00:00"),
    filter(raw, date >= "2025-03-09 02:00:00") |>
      mutate(date = date - 3600)
  )
  
  ## 6 ── 15-min averages ----------------------------------------------------
  ramp_15 <- openair::timeAverage(raw, avg.time = avg_time)
  
  ## predictors --------------------------------------------------------------
  gas_mat <- ramp_15 |>
    select(CO_RAMP, NO_RAMP, NO2_RAMP, O3_RAMP,
           CO2_RAMP, T_RAMP, RH_RAMP) |>
    as.matrix()
  
  pm_mat  <- ramp_15 |>
    select(T_RAMP, RH_RAMP) |>
    mutate(DP_RAMP = 243.12 *
             (log(RH_RAMP/100) + 17.62*T_RAMP / (243.12 + T_RAMP)) /
             (17.62 - (log(RH_RAMP/100) + 17.62*T_RAMP / (243.12 + T_RAMP)))) |>
    as.matrix()
  
  ## 7 ── Apply CAPS models --------------------------------------------------
  message("→ Applying calibration …")
  pred <- tibble(
    date  = ramp_15$date,
    NO2   = as.numeric(CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$NO2,  gas_mat)),
    NO    = as.numeric(CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$NO,   gas_mat)),
    CO2   = as.numeric(CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$CO2,  gas_mat)),
    O3    = as.numeric(CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$O3,   gas_mat)),
    CO    = as.numeric(CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$CO,   gas_mat)),
    PM2_5 = as.numeric(CAPS_PR_Apply   (calibration_models$pm$Regression$PM2_5, pm_mat))
  )
  
  ## 8 ── Optional write-out -------------------------------------------------
  if (!is.null(out_dir)) {
    fs::dir_create(out_dir)
    out_file <- file.path(
      out_dir,
      sprintf("%s_%s_calibrated.csv",
              sensor_id,
              tools::file_path_sans_ext(basename(data_file)))
    )
    readr::write_csv(pred, out_file)
    message("✔ Calibrated file saved ➜ ", out_file)
  }
  
  invisible(pred)
}
