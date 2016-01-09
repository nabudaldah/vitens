
##############################
########## Libraries #########
##############################

library(ggplot2)
library(stringr)
library(lubridate)
library(magrittr)
library(dplyr)
library(tidyr)
library(data.table)
library(foreach)
library(doParallel)
library(fasttime)
library(tm)
library(wordcloud)
library(RCurl)
library(RJSONIO)
library(reshape)
library(ggplot2)
library(maptools)
library(mapproj)

##############################
########## Globals ###########
##############################

# Cache folder
temp_folder <- 'c:/temp/vitens'
data_folder <- paste0(temp_folder, '/0.data')
meta_folder <- paste0(temp_folder, '/1.meta')
conv_folder <- paste0(temp_folder, '/2.conv')
aggr_folder <- paste0(temp_folder, '/3.aggr')
cast_folder <- paste0(temp_folder, '/4.cast')

# Create folders
dir.create(temp_folder, showWarnings = F, recursive = T)
dir.create(data_folder, showWarnings = F, recursive = T)
dir.create(meta_folder, showWarnings = F, recursive = T)
dir.create(conv_folder, showWarnings = F, recursive = T)
dir.create(aggr_folder, showWarnings = F, recursive = T)
dir.create(cast_folder, showWarnings = F, recursive = T)
stopifnot(dir.exists(temp_folder))
stopifnot(dir.exists(data_folder))
stopifnot(dir.exists(meta_folder))
stopifnot(dir.exists(conv_folder))
stopifnot(dir.exists(aggr_folder))
stopifnot(dir.exists(cast_folder))

# Log file (for checking progress)
log_file <- paste0(temp_folder, '/debug.txt')

# Meta file
meta_file <- paste0(meta_folder, '/meta.RData')
data_file <- paste0(cast_folder, '/data.RData')
data_table_file <- paste0(cast_folder, '/data_table.RData')

##############################
########## Functions #########
##############################

CopyDf <- function(x, sep = '\t', newline = '\n'){
	hd <- paste0(colnames(x), collapse = sep)
	x %>%
		apply(1, function(row) paste0(row, collapse = sep)) %>%
		c(hd, .) %>%
		paste0(collapse = newline) %>%
		writeClipboard
}
