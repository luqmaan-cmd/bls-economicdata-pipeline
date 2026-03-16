# BLS Bulk Download Script using BLSloadR
# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

# Install package if not already installed
if (!require("BLSloadR", quietly = TRUE)) {
  install.packages("BLSloadR")
}

library(BLSloadR)

# Datasets to download: ce, cu, jt, pr, pi, ci, sm, la
cat("=== Downloading BLS Datasets ===\n\n")

# 1. CE - Current Employment Statistics (National)
cat("=== [1/8] CE - Current Employment Statistics (National) ===\n")
ce_data <- get_national_ces()
cat(sprintf("Records: %d\n\n", nrow(ce_data)))
write.csv(ce_data, "bls_ce_data.csv", row.names = FALSE)

# 2. CU - Consumer Price Index (Current data)
cat("=== [2/8] CU - Consumer Price Index ===\n")
cu_data <- load_bls_dataset("cu", which_data = "current")
if (!is.null(cu_data$data)) {
  cat(sprintf("Records: %d\n\n", nrow(cu_data$data)))
  write.csv(cu_data$data, "bls_cu_data.csv", row.names = FALSE)
}

# 3. JT - Job Openings (JOLTS)
cat("=== [3/8] JT - Job Openings (JOLTS) ===\n")
jt_data <- get_jolts()
cat(sprintf("Records: %d\n\n", nrow(jt_data)))
write.csv(jt_data, "bls_jt_data.csv", row.names = FALSE)

# 4. PR - Producer Price Index
cat("=== [4/8] PR - Producer Price Index ===\n")
pr_data <- load_bls_dataset("pr", which_data = "current")
if (!is.null(pr_data$data)) {
  cat(sprintf("Records: %d\n\n", nrow(pr_data$data)))
  write.csv(pr_data$data, "bls_pr_data.csv", row.names = FALSE)
}

# 5. PI - Import/Export Price Indexes
cat("=== [5/8] PI - Import/Export Price Indexes ===\n")
pi_data <- load_bls_dataset("pi", which_data = "current")
if (!is.null(pi_data$data)) {
  cat(sprintf("Records: %d\n\n", nrow(pi_data$data)))
  write.csv(pi_data$data, "bls_pi_data.csv", row.names = FALSE)
}

# 6. CI - Employment Cost Index
cat("=== [6/8] CI - Employment Cost Index ===\n")
ci_data <- load_bls_dataset("ci", which_data = "current")
if (!is.null(ci_data$data)) {
  cat(sprintf("Records: %d\n\n", nrow(ci_data$data)))
  write.csv(ci_data$data, "bls_ci_data.csv", row.names = FALSE)
}

# 7. SM - State and Area Employment (CES State)
cat("=== [7/8] SM - State and Area Employment ===\n")
sm_data <- get_ces()
cat(sprintf("Records: %d\n\n", nrow(sm_data)))
write.csv(sm_data, "bls_sm_data.csv", row.names = FALSE)

# 8. LA - Local Area Unemployment Statistics
cat("=== [8/8] LA - Local Area Unemployment Statistics ===\n")
la_data <- get_laus(geography = "state_current_adjusted")
cat(sprintf("Records: %d\n\n", nrow(la_data)))
write.csv(la_data, "bls_la_data.csv", row.names = FALSE)

cat("=== All Downloads Complete ===\n")
cat("Saved files:\n")
cat("  bls_ce_data.csv\n")
cat("  bls_cu_data.csv\n")
cat("  bls_jt_data.csv\n")
cat("  bls_pr_data.csv\n")
cat("  bls_pi_data.csv\n")
cat("  bls_ci_data.csv\n")
cat("  bls_sm_data.csv\n")
cat("  bls_la_data.csv\n")
