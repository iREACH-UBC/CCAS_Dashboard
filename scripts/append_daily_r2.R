#!/usr/bin/env Rscript
# append_to_r2_csv.R — Multi-sensor monthly appends to Cloudflare R2 (S3-compatible)
#
# Features
#   • Process many sensors in one run: --sensors "2021,2022,2040"
#   • Two input modes:
#       (A) Per-sensor input template: --input-template "calibrated_data/{sensor}/latest.csv"
#       (B) Single input file with all sensors: --input data/all.csv --sensor-col SENSOR_ID
#   • Monthly partitioned output: --key-template "cc/data/{sensor}/{yyyymm}.csv"
#     -> rows are routed by floor_date(--date-col, 'month'), using YYYYMM from row dates
#   • Portion selectors (applied after per-sensor filtering): --tail, --head, --since/--until
#   • Merge with existing object if present; else create new
#   • Optional de-duplication (--dedupe-on COLS) and final sort (--sort-by COL)
#   • --dry-run to preview without upload
#
# Env vars fallback:
#   R2_ENDPOINT, R2_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
#
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(lubridate)
  library(paws.storage)
  library(stringr)
  library(purrr)
})

# ── CLI parsing --------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
get_val <- function(flag, default = NULL) {
  i <- which(args == flag)
  if (length(i) == 0) return(default)
  if (i[1] == length(args)) stop(paste0("Missing value for ", flag))
  args[i[1] + 1]
}
has_flag <- function(flag) any(args == flag)

parse_cols <- function(x) {
  if (is.null(x) || !nzchar(x)) return(character(0))
  unique(trimws(unlist(strsplit(x, ","))))
}

opt <- list(
  sensors       = parse_cols(get_val("--sensors", NULL)),
  input         = get_val("--input", NULL),
  input_tmpl    = get_val("--input-template", NULL),
  sensor_col    = get_val("--sensor-col", NULL),
  bucket        = get_val("--bucket", Sys.getenv("R2_BUCKET", "")),
  key_tmpl      = get_val("--key-template", NULL),
  endpoint      = get_val("--endpoint", Sys.getenv("R2_ENDPOINT", "")),
  region        = get_val("--region", Sys.getenv("AWS_DEFAULT_REGION", "auto")),
  encoding      = get_val("--encoding", "UTF-8"),
  date_col      = get_val("--date-col", NULL),
  tail          = get_val("--tail", NULL),
  head          = get_val("--head", NULL),
  since         = get_val("--since", NULL),
  until         = get_val("--until", NULL),
  keep_cols     = parse_cols(get_val("--keep-cols", NULL)),
  dedupe_on     = parse_cols(get_val("--dedupe-on", NULL)),
  sort_by       = get_val("--sort-by", NULL),
  dry_run       = has_flag("--dry-run")
)

# Validation
if (!length(opt$sensors)) stop("--sensors is required (comma-separated)")
if (nzchar(opt$bucket) == FALSE) stop("--bucket or $R2_BUCKET is required")
if (nzchar(opt$endpoint) == FALSE) stop("--endpoint or $R2_ENDPOINT is required")
if (is.null(opt$key_tmpl)) stop("--key-template is required and must include {sensor} and {yyyymm}")
if (!str_detect(opt$key_tmpl, fixed("{sensor}"))) stop("--key-template must contain {sensor}")
if (!str_detect(opt$key_tmpl, fixed("{yyyymm}"))) stop("--key-template must contain {yyyymm}")
if (is.null(opt$date_col) || !nzchar(opt$date_col)) stop("--date-col is required (used for monthly partitioning)")
if (!is.null(opt$tail) && !is.null(opt$head)) stop("Use only one of --head or --tail")
opt$tail <- if (!is.null(opt$tail)) as.integer(opt$tail) else NA_integer_
opt$head <- if (!is.null(opt$head)) as.integer(opt$head) else NA_integer_

mode <- if (!is.null(opt$input_tmpl)) "template" else if (!is.null(opt$input) && !is.null(opt$sensor_col)) "single" else NA_character_
if (is.na(mode)) {
  stop("Specify either:\n  (A) --input-template with {sensor}\n  OR\n  (B) --input plus --sensor-col (column holding sensor IDs)")
}

# Date parse helper
parse_dates <- function(x) {
  orders <- c("Ymd HMS","Y-m-d H:M:S","Y-m-d H:M","Y-m-d",
              "Ymd HMSz","Y-m-dTH:M:S","Y-m-dTH:M:Sz",
              "mdY HMS","mdY HM","mdY")
  suppressWarnings(lubridate::parse_date_time(x, orders = orders, quiet = TRUE))
}

# Portion select
apply_portion <- function(df) {
  if (length(opt$keep_cols)) {
    keep <- intersect(opt$keep_cols, names(df))
    missing <- setdiff(opt$keep_cols, keep)
    if (length(missing)) message("[warn] --keep-cols ignored missing: ", paste(missing, collapse = ", "))
    if (length(keep)) df <- dplyr::select(df, dplyr::all_of(keep))
  }
  if (!is.null(opt$since) || !is.null(opt$until)) {
    if (!(opt$date_col %in% names(df))) stop("--date-col '", opt$date_col, "' not found in input")
    dates <- parse_dates(df[[opt$date_col]])
    mask <- rep(TRUE, nrow(df))
    if (!is.null(opt$since)) mask <- mask & (dates >= parse_dates(opt$since))
    if (!is.null(opt$until)) mask <- mask & (dates <  parse_dates(opt$until))
    before <- nrow(df)
    df <- df[mask, , drop = FALSE]
    message("[info] date filter kept ", nrow(df), "/", before, " rows")
  }
  if (!is.na(opt$head)) df <- head(df, opt$head)
  if (!is.na(opt$tail)) df <- tail(df, opt$tail)
  df
}

# S3 client
s3 <- paws.storage::s3(config = list(
  endpoint = opt$endpoint,
  region   = opt$region,
  s3_force_path_style = TRUE
))

object_exists <- function(bucket, key) {
  out <- tryCatch({
    s3$head_object(Bucket = bucket, Key = key); TRUE
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (grepl("404|NotFound|NoSuchKey|Not Found", msg, ignore.case = TRUE)) return(FALSE)
    if (grepl("403|Forbidden|AccessDenied", msg, ignore.case = TRUE)) stop("[auth] 403 Forbidden for ", key, " — check credentials/policy.")
    stop(e)
  })
  out
}
download_object_to <- function(bucket, key, dst) {
  obj <- s3$get_object(Bucket = bucket, Key = key)
  con <- file(dst, "wb"); on.exit(close(con), add = TRUE)
  writeBin(obj$Body, con)
}
upload_file <- function(src, bucket, key) {
  raw <- readBin(src, what = "raw", n = file.size(src))
  s3$put_object(Bucket = bucket, Key = key, Body = raw, ContentType = "text/csv")
}

# temp dir
td <- tempfile("r2csv"); dir.create(td, recursive = TRUE, showWarnings = FALSE)

# Input loader per sensor
load_input_for_sensor <- function(sensor_id) {
  if (mode == "template") {
    path <- gsub("\\{sensor\\}", sensor_id, opt$input_tmpl, fixed = TRUE)
    message("[info] reading input for sensor ", sensor_id, ": ", path)
    df <- tryCatch(
      readr::read_csv(path, locale = readr::locale(encoding = opt$encoding), show_col_types = FALSE),
      error = function(e) stop("Failed to read input CSV for sensor ", sensor_id, ": ", e$message)
    )
  } else {
    # single file, filter by sensor_col
    if (is.null(opt$input) || is.null(opt$sensor_col)) stop("single-file mode requires --input and --sensor-col")
    message("[info] reading input file (single mode): ", opt$input)
    df_all <- tryCatch(
      readr::read_csv(opt$input, locale = readr::locale(encoding = opt$encoding), show_col_types = FALSE),
      error = function(e) stop("Failed to read input CSV: ", e$message)
    )
    if (!(opt$sensor_col %in% names(df_all))) stop("--sensor-col '", opt$sensor_col, "' not found in input")
    df <- dplyr::filter(df_all, .data[[opt$sensor_col]] == sensor_id)
  }
  df
}

merge_and_upload <- function(df_in, sensor_id) {
  if (!(opt$date_col %in% names(df_in))) {
    stop("For sensor ", sensor_id, ": --date-col '", opt$date_col, "' not found in input")
  }
  # Parse month keys
  dates <- parse_dates(df_in[[opt$date_col]])
  if (all(is.na(dates))) stop("For sensor ", sensor_id, ": could not parse any dates in column ", opt$date_col)
  months <- floor_date(dates, unit = "month")
  yyyymm <- format(months, "%Y%m")
  df_in$.__yyyymm <- yyyymm
  
  # For each month present, append to the corresponding key
  split(df_in, df_in$.__yyyymm) %>% imap(function(chunk, ym) {
    key <- opt$key_tmpl %>%
      gsub("\\{sensor\\}", sensor_id, ., fixed = TRUE) %>%
      gsub("\\{yyyymm\\}", ym, ., fixed = TRUE)
    message("[info] sensor ", sensor_id, " -> ", key, " (rows: ", nrow(chunk), ")")
    
    # Drop helper col for downstream
    chunk <- dplyr::select(chunk, -.__yyyymm)
    
    # Portion selectors have already been applied globally; but they might cross months.
    # Here we accept the pre-filtered chunk.
    
    # Download existing if any
    existing_path <- file.path(td, paste0("existing_", sensor_id, "_", ym, ".csv"))
    merged_path   <- file.path(td, paste0("merged_",   sensor_id, "_", ym, ".csv"))
    if (object_exists(opt$bucket, key)) {
      message("[info] downloading existing object: s3://", opt$bucket, "/", key)
      download_object_to(opt$bucket, key, existing_path)
      df_existing <- tryCatch(
        readr::read_csv(existing_path, locale = readr::locale(encoding = opt$encoding), show_col_types = FALSE),
        error = function(e) stop("Failed to read existing CSV for ", key, ": ", e$message)
      )
    } else {
      df_existing <- chunk[0, , drop = FALSE]  # empty with same cols
    }
    
    # Align columns (union)
    all_cols <- union(names(df_existing), names(chunk))
    df_existing <- df_existing %>% dplyr::mutate(across(everything(), identity)) %>% dplyr::select(dplyr::all_of(all_cols))
    chunk       <- chunk       %>% dplyr::mutate(across(everything(), identity)) %>% dplyr::select(dplyr::all_of(all_cols))
    
    df_merged <- dplyr::bind_rows(df_existing, chunk)
    
    if (length(opt$dedupe_on)) {
      missing_keys <- setdiff(opt$dedupe_on, names(df_merged))
      if (length(missing_keys)) stop("--dedupe-on columns missing after merge for ", key, ": ", paste(missing_keys, collapse = ","))
      before <- nrow(df_merged)
      df_merged <- df_merged %>%
        dplyr::group_by(dplyr::across(dplyr::all_of(opt$dedupe_on))) %>%
        dplyr::slice_tail(n = 1) %>%
        dplyr::ungroup()
      message("[info] de-duplicated ", before - nrow(df_merged), " rows on {", paste(opt$dedupe_on, collapse = ","), "} for ", key)
    }
    
    if (!is.null(opt$sort_by) && nzchar(opt$sort_by)) {
      if (!(opt$sort_by %in% names(df_merged))) stop("--sort-by column not found in merged data for ", key, ": ", opt$sort_by)
      col <- df_merged[[opt$sort_by]]
      if (is.character(col)) {
        parsed <- suppressWarnings(parse_dates(col))
        if (any(!is.na(parsed))) {
          df_merged <- df_merged %>% dplyr::arrange(parsed)
        } else {
          df_merged <- df_merged %>% dplyr::arrange(.data[[opt$sort_by]])
        }
      } else {
        df_merged <- df_merged %>% dplyr::arrange(.data[[opt$sort_by]])
      }
    }
    
    if (opt$dry_run) {
      message("[dry-run] ", key, " tail(5):")
      utils::capture.output(utils::tail(df_merged, 5), file = stdout())
      return(invisible(NULL))
    }
    
    readr::write_csv(df_merged, merged_path, na = "", progress = FALSE)
    message("[info] uploading merged CSV to s3://", opt$bucket, "/", key)
    upload_file(merged_path, opt$bucket, key)
    message("[success] uploaded ", key)
    invisible(NULL)
  })
}

# ── Main loop ---------------------------------------------------------------
for (sid in opt$sensors) {
  df_raw <- load_input_for_sensor(sid)
  df_flt <- apply_portion(df_raw)
  if (nrow(df_flt) == 0) {
    message("[info] sensor ", sid, ": no rows after filtering; skipping")
    next
  }
  merge_and_upload(df_flt, sid)
}

message("[done] all sensors processed")
