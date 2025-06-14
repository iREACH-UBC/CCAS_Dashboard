args <- commandArgs(trailingOnly = TRUE)
sensor_id   <- args[1]
input_file  <- args[2]
output_file <- file.path("calibrated_data", sensor_id, paste0(sensor_id, "_calibrated_output.csv"))
model_file  <- file.path("calibration_models", sensor_id, "Calibration_Models.obj")

# ---- Load packages ----
library(readr)
library(dplyr)

# ---- Load calibration model ----
if (!file.exists(model_file)) {
  stop("Calibration model not found for sensor ", sensor_id)
}
load(model_file)

# ---- Load input data ----
df <- read_csv(input_file, show_col_types = FALSE)

# ---- Define model application functions ----

is_empty <- function(x) {
  is.null(x) || length(x) == 0 || all(is.na(x))
}

CAPS_PR_Apply <- function(model_object, m_data) {
  if (is_empty(m_data) || is_empty(model_object)) {
    return(matrix(NaN, nrow(m_data), 1))
  }
  
  m_data_new <- matrix(1, nrow(m_data), ncol(m_data) + 1)
  m_data_new[, 2:(ncol(m_data) + 1)] <- m_data
  m_data <- m_data_new
  
  parameters <- as.matrix(model_object)
  n_parameters <- length(parameters)
  
  n_order <- 0
  if (n_parameters == 1) {
    n_order <- 0
  } else if (n_parameters == ncol(m_data)) {
    n_order <- 1
  } else if (n_parameters == sum(1:ncol(m_data))) {
    n_order <- 2
  } else {
    i_order <- 3
    while (n_order == 0) {
      m_combinations <- gtools::combinations(ncol(m_data), i_order, repeats.allowed = TRUE)
      if (n_parameters == nrow(m_combinations)) {
        n_order <- i_order
      } else {
        i_order <- i_order + 1
      }
    }
  }
  
  m_combinations <- gtools::combinations(ncol(m_data), n_order, repeats.allowed = TRUE)
  X <- matrix(NA, nrow(m_data), nrow(m_combinations))
  
  for (i in 1:nrow(m_combinations)) {
    X[, i] <- apply(m_data[, m_combinations[i, ], drop = FALSE], 1, prod)
  }
  
  if (any(parameters == 0)) {
    keep <- parameters != 0
    X <- X[, keep, drop = FALSE]
    parameters <- parameters[keep]
  }
  
  result <- X %*% parameters
  result[is.na(result)] <- NaN
  return(result)
}

CAPS_RF_Apply <- function(model_object, m_data) {
  if (is_empty(model_object) || is_empty(m_data)) {
    return(matrix(NaN, nrow(m_data), 1))
  }
  
  library(randomForest)
  c_input_names <- paste0("input", seq_len(ncol(m_data)))
  f_data <- as.data.frame(m_data)
  colnames(f_data) <- c_input_names
  
  v_data <- rep(NaN, nrow(f_data))
  valid <- complete.cases(f_data)
  if (any(valid)) {
    v_data[valid] <- predict(model_object, newdata = f_data[valid, ])
  }
  
  detach("package:randomForest", unload = TRUE)
  return(matrix(v_data, ncol = 1))
}

CAPS_Hybrid_Apply <- function(model_object, m_data) {
  if (is_empty(model_object) || is_empty(m_data)) {
    return(matrix(NaN, nrow(m_data), 1))
  }
  
  bounds <- model_object[[1]]
  RF_model <- model_object[[2]]
  lower_LR_model <- model_object[[3]]
  upper_LR_model <- model_object[[4]]
  
  c_input_names <- paste0("input", seq_len(ncol(m_data)))
  colnames(m_data) <- c_input_names
  
  v_data <- CAPS_RF_Apply(RF_model, m_data)
  
  below <- which(v_data < bounds[1])
  above <- which(v_data > bounds[2])
  
  if (length(below) > 0 && !is_empty(lower_LR_model)) {
    v_data[below] <- CAPS_PR_Apply(lower_LR_model, m_data[below, , drop = FALSE])
  }
  if (length(above) > 0 && !is_empty(upper_LR_model)) {
    v_data[above] <- CAPS_PR_Apply(upper_LR_model, m_data[above, , drop = FALSE])
  }
  
  return(matrix(v_data, ncol = 1))
}

# ---- Apply calibration models ----
gas_cols <- c("CO", "NO", "NO2", "O3", "CO2", "T", "RH")
pm_cols  <- c("T", "RH")

gas_mat <- as.matrix(df[, gas_cols])
pm_mat  <- as.matrix(df[, pm_cols])

df <- df %>%
  mutate(
    NO2   = CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$NO2, gas_mat),
    NO    = CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$NO, gas_mat),
    CO2   = CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$CO2, gas_mat),
    O3    = CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$O3, gas_mat),
    CO    = CAPS_Hybrid_Apply(calibration_models$gas$Hybrid$CO, gas_mat),
    `PM2.5` = CAPS_PR_Apply(calibration_models$pm$Regression$PM2_5, pm_mat)
  )

# ---- Save output ----
write_csv(df, output_file)
cat("✅ Calibrated data written to", output_file, "\n")
