# Optimized version of 1_DataExtract.R
library(tidyverse)
#devtools::install_github("danicat/read.dbc")
library(read.dbc)
library(data.table) # More efficient for large data operations

setwd("D:/DATASUS/SIM/Dados")
files <- list.files(path="D:/DATASUS/SIM/Dados", pattern="*.dbc", full.names=TRUE, recursive=FALSE, ignore.case = TRUE)
files <- files[as.numeric(substring(files, 26,30)) %in% c(2009:2023)]
files <- files[substring(files, 24,25) != "BR"]

cid_df <- tribble(
  ~cid,   ~descricao,
  # --- Doença de Chagas ------------------------------------------------
  "B570","Doença de Chagas (forma cardíaca aguda)",
  "B572","Doença de Chagas (forma cardíaca crônica)",
  # --- IAM / Infarto ---------------------------------------------------
  "I21",  "Infarto agudo do miocárdio",
  "I22",  "Infarto do miocárdio recorrente",
  # --- Edema agudo de pulmão ------------------------------------------
  "J81",  "Edema agudo de pulmão",
  # --- Parada cardiorrespiratória / morte súbita ----------------------
  "I461","Parada cardíaca devida a condição subjacente",
  "I469","Parada cardíaca, não especificada",
  "R960","Morte instantânea",
  "R961","Morte ocorrida em < 24 h do início",
  "R97",  "Morte inexplicada, aguardando investigação",
  "R98",  "Morte sem assistência (não testemunhada)",
  "R99",  "Causa de morte mal definida",
  # --- Dissecção de aorta ---------------------------------------------
  "I710","Dissecção da aorta torácica",
  "I711","Dissecção da aorta abdominal",
  # --- Insuficiência cardíaca -----------------------------------------
  "I50",  "Insuficiência cardíaca",
  # --- Outras doenças cardíacas agudas (I30–I52) -----------------------
  "I30",  "Pericardite aguda",
  "I31",  "Outras doenças do pericárdio",
  "I32",  "Pericardite em doenças classificadas em outra parte",
  "I33",  "Endocardite aguda e subaguda",
  "I34",  "Doenças não reumáticas da valva mitral",
  "I35",  "Doenças não reumáticas da valva aórtica",
  "I36",  "Doenças não reumáticas da valva tricúspide",
  "I37",  "Doenças da valva pulmonar",
  "I38",  "Endocardite de valva, não especificada",
  "I39",  "Endocardite/valvopatias em doenças de outra parte",
  "I40",  "Miocardite aguda",
  "I41",  "Miocardite em doenças classificadas em outra parte",
  "I42",  "Cardiomiopatia",
  "I43",  "Cardiomiopatia em doenças classificadas em outra parte",
  "I44",  "Bloqueio A-V e bloqueio de ramo esquerdo",
  "I45",  "Outros distúrbios de condução",
  "I46",  "Parada cardíaca",
  "I47",  "Taquicardia paroxística",
  "I48",  "Fibrilação e flutter atrial",
  "I49",  "Outras arritmias cardíacas",
  "I51",  "Outras doenças cardíacas especificadas",
  "I52",  "Doenças cardíacas em afecções de outra parte"
)


# --- Build helper vectors once ----------------------------------------------
CIDs_clean <- gsub("\\.", "", cid_df$cid)
CIDs_3     <- CIDs_clean[nchar(CIDs_clean) == 3]   # e.g. I21
CIDs_4     <- CIDs_clean[nchar(CIDs_clean) == 4]   # e.g. B570 / I461

# --- Fully-vectorised filter --------------------------------------------------
filter_cids <- function(dt) {
    # All 3- and 4-character slices we need
    slices <- list(
        substr(dt$CAUSABAS, 1, 3), substr(dt$CAUSABAS, 1, 4),
        substr(dt$LINHAA,  2, 4),  substr(dt$LINHAA,  2, 5),
        substr(dt$LINHAB,  2, 4),  substr(dt$LINHAB,  2, 5),
        substr(dt$LINHAC,  2, 4),  substr(dt$LINHAC,  2, 5),
        substr(dt$LINHAD,  2, 4),  substr(dt$LINHAD,  2, 5),
        substr(dt$LINHAII, 2, 4),  substr(dt$LINHAII, 2, 5)
    )
    
    # Which of the 12 slices are 3- vs 4-chars?
    is3 <- rep(c(TRUE, FALSE), 6)   # every odd element is 3-char
    
    # Logical matrix: each column = one slice
    m <- do.call(cbind, lapply(seq_along(slices), function(i)
        if (is3[i]) slices[[i]] %chin% CIDs_3 else slices[[i]] %chin% CIDs_4))
    
    dt[rowSums(m) > 0]              # keep rows with at least one match
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
    # Load required libraries in each worker
    library(data.table)
    library(read.dbc)
    
    t <- setDT(read.dbc(.x, as.is = TRUE))
    filter_cids(t)
  }, .options = furrr_options(globals = c("filter_cids", "CIDs_3", "CIDs_4")))
  
  plan(sequential) # Reset to sequential processing
  return(SIM)
}

# Execute the optimized version (choose one method)
cat("Starting optimized data processing...\n")
start_time <- Sys.time()

# Use the data.table version for best performance
#SIM <- process_files_dt()   #5min
SIM <- process_files_parallel() # 3.7min with parallel processing


end_time <- Sys.time()
cat("Processing completed in:", round(difftime(end_time, start_time, units = "mins"), 2), "minutes\n")
cat("Total rows in final dataset:", nrow(SIM), "\n")

# Save results
setwd("C:\\Users\\bcast\\Documents\\Analises Hobby\\INCOR\\Mariana")
#save(SIM, file = "SIM Dcard_Optimized.RData")

# Optional: Save as more efficient formats
# Save as parquet - most efficient for large datasets
library(arrow)
write_parquet(SIM, "SIM Dcard_Optimized.parquet")

# Alternative efficient formats:
# saveRDS(SIM, file = "SIM Dcard_Optimized.rds", compress = "xz") # Better compression
# library(qs)
# qsave(SIM, "SIM Dcard_Optimized.qs") # Faster than RDS with good compression
# fwrite(SIM, file = "SIM Dcard_Optimized.csv") # If you need CSV