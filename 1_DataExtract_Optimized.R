# Optimized version of 1_DataExtract.R
library(tidyverse)
library(read.dbc)
library(data.table) # More efficient for large data operations

setwd("D:/DATASUS/SIM/Dados")
files <- list.files(path="D:/DATASUS/SIM/Dados", pattern="*.dbc", full.names=TRUE, recursive=FALSE, ignore.case = TRUE)
files <- files[as.numeric(substring(files, 26,30)) %in% c(2009:2021)]
files <- files[substring(files, 24,25) != "BR"]

CIDs <- c("B57","I21", "I22", "I46", "I44", "I49", "I71", "I50", "I42", "J81", "R96", "R99")

# Optimized filtering function
filter_cids <- function(dt) {
  # Pre-compute all substring operations once
  causabas_substr <- substring(dt$CAUSABAS, 1, 3)
  linhaa_substr <- substring(dt$LINHAA, 2, 4)
  linhab_substr <- substring(dt$LINHAB, 2, 4)
  linhac_substr <- substring(dt$LINHAC, 2, 4)
  linhad_substr <- substring(dt$LINHAD, 2, 4)
  linhaii_substr <- substring(dt$LINHAII, 2, 4)
  
  # Create logical vector with single pass
  keep <- causabas_substr %in% CIDs | 
          linhaa_substr %in% CIDs |
          linhab_substr %in% CIDs | 
          linhac_substr %in% CIDs |
          linhad_substr %in% CIDs | 
          linhaii_substr %in% CIDs
  
  return(dt[keep, ])
}

# Option 1: Using data.table for maximum efficiency
process_files_dt <- function() {
  SIM_list <- vector("list", length(files))
  
  for(i in seq_along(files)) {
    cat("Processing file", i, "of", length(files), "...")
    
    # Read as data.table for faster operations
    t <- setDT(read.dbc(files[i], as.is = TRUE))
    
    # Apply optimized filter
    t_filtered <- filter_cids(t)
    SIM_list[[i]] <- t_filtered
    
    cat(" Done. Rows kept:", nrow(t_filtered), "\n")
  }
  
  # More efficient binding using data.table
  SIM <- rbindlist(SIM_list, fill = TRUE)
  return(SIM)
}

# Option 2: Using map_dfr for cleaner code (slightly less efficient but more readable)
process_files_tidy <- function() {
  map_dfr(files, ~ {
    cat("Processing:", basename(.x), "\n")
    t <- read.dbc(.x, as.is = TRUE)
    filter_cids(t)
  })
}

# Option 3: Parallel processing for multiple cores (if you have many files)
process_files_parallel <- function() {
  library(furrr)
  library(future)
  
  # Set up parallel processing
  plan(multisession, workers = parallel::detectCores() - 1)
  
  SIM <- future_map_dfr(files, ~ {
    t <- read.dbc(.x, as.is = TRUE)
    filter_cids(t)
  })
  
  plan(sequential) # Reset to sequential processing
  return(SIM)
}

# Execute the optimized version (choose one method)
cat("Starting optimized data processing...\n")
start_time <- Sys.time()

# Use the data.table version for best performance
SIM <- process_files_dt()

end_time <- Sys.time()
cat("Processing completed in:", round(difftime(end_time, start_time, units = "mins"), 2), "minutes\n")
cat("Total rows in final dataset:", nrow(SIM), "\n")

# Save results
setwd("C:\\Users\\bcast\\Documents\\Analises Hobby\\INCOR\\Mariana")
save(SIM, file = "SIM Dcard_Optimized.RData")

# Optional: Save as more efficient formats
# saveRDS(SIM, file = "SIM Dcard_Optimized.rds") # More efficient than .RData
# fwrite(SIM, file = "SIM Dcard_Optimized.csv") # If you need CSV