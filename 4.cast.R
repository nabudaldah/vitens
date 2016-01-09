
##############################
########## Settings ##########
##############################

time_0 <- as.POSIXct('2014-01-01 00:00:00', format = '%Y-%m-%d %H:%M:%S', tz = 'UTC')
time_1 <- as.POSIXct('2015-12-31 23:59:59', format = '%Y-%m-%d %H:%M:%S', tz = 'UTC')
cache_filename_pattern <- ''

# Status translation
status_translate = list('geen storing' = '0', 'storing' = 1, 'in bedrijf' = '1', 'uit bedrijf' = '0', 'dicht' = '1', 'niet dicht' = 0, 'open' = 1, 'niet open' = 0)

##############################
########## Setup    ##########
##############################

# Load libraries
source('./0.setup.R')

# Load meta-data
load(meta_file)

##############################
########## Cluster ###########
##############################

# Startup parallel workers
cores <- detectCores()
cl <- makeCluster(cores, outfile = log_file)
clusterEvalQ(cl, source('./0.setup.R'))

##############################
########## Work function #####
##############################

# Work to be done per item
work <- function(item){
	load(paste0(aggr_folder, '/', item))
	df_meta <- meta %>% subset(str_replace(id, '.csv', '.RData') == item)
	df %<>% subset(time >= time_0 & time <= time_1)
	return(df)
}

# Share settings with every cluster worker
clusterExport(cl, list('temp_folder', 'conv_folder', 'meta', 'work', 'aggr_folder', 'time_0', 'time_1', 'status_translate'))

##############################
########## Execute work ######
##############################

# Apply parallel
items <- dir(aggr_folder, cache_filename_pattern)
results <- parLapplyLB(cl, items, function(item){
	print(item)

	result <- tryCatch({
		aggr_file <- paste0(aggr_folder, '/', str_replace(item, '[.]RData', '.RData'))
		df <- work(item)
		return(list(item = item, result = df,  error = ''))
	}, error = function(err){
		return(list(item = item, result = 'err', error = err$message))
	})

	return(result)

})

# Shut down cluster workers (!)
tryCatch({ stopCluster(cl) }, error = warning)

##############################
########## Results ###########
##############################

#results %<>% rbindlist

data <- results %>%
	Filter(function(result) result[['error']] == '', .) %>%
	Map(function(result) result[['result']], .) %>%
	rbindlist

errors <- results %>%
	Filter(function(result) result[['error']] != '', .) %>%
	Map(function(result) result[['error']], .) %>%
	rbindlist

# Save data
save(data, file = data_file)
data_file_csv   <- paste0(data_file, '.csv')
write.table(data, data_file_csv, quote = F, sep = ';', row.names = F, na = '')

# Cast data and save
data_table <- data %>% dcast(time ~ id)
save(data_table, file = data_table_file)
data_table_file_csv <- paste0(data_table_file, '.csv')
write.table(data_table, data_table_file_csv, quote = F, sep = ';', row.names = F, na = '')

