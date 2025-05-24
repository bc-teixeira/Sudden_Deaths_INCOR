library(tidyverse)
library(timetk)
library(foreign)
library(readxl)
library(lubridate)
library(read.dbc)

setwd("D:/DATASUS/SIM/Dados")
files <- list.files(path="D:/DATASUS/SIM/Dados", pattern="*.dbc", full.names=TRUE, recursive=FALSE, ignore.case = TRUE)
files <- files[as.numeric(substring(files, 26,30)) %in% c(2009:2021)]
files <- files[substring(files, 24,25) != "BR"]


SIM <- vector("list", length(files))
CIDs <- c("B57","I21", "I22", "I46", "I44", "I49", "I71", "I50", "I42", "J81", "R96", "R99")

for(i in 1:length(files)){
  t <- read.dbc(files[i], as.is = TRUE) # load file

  # apply function
t <- t %>%  filter(substring(CAUSABAS, 1, 3) %in% CIDs | substring(LINHAA, 2, 4) %in% CIDs |
                     substring(LINHAB, 2, 4) %in% CIDs | substring(LINHAC, 2, 4) %in% CIDs |
                     substring(LINHAD, 2, 4) %in% CIDs | substring(LINHAII, 2, 4) %in% CIDs)



#SIM <- plyr::rbind.fill(SIM, t)
SIM[[i]] <- t

cat(round(i*100/length(files),1))
}
SIM <- plyr::rbind.fill(SIM)

setwd("C:\\Users\\bcast\\Documents\\Analises Hobby\\INCOR\\Mariana")
save(SIM, file = "SIM Dcard.RData")
