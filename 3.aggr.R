
##############################
########## Settings ##########
##############################

# Slice time from time_0 to time_1 including time_0 and time_1
time_0 <- as.POSIXct('2014-01-01 00:00:00', format = '%Y-%m-%d %H:%M:%S', tz = 'UTC')
time_1 <- as.POSIXct('2016-06-30 23:59:59', format = '%Y-%m-%d %H:%M:%S', tz = 'UTC')
cache_filename_pattern <- ''
time_rounding <- 'hour'

##############################
########## Setup    ##########
##############################

# Load libraries
source('./0.setup.R')

# Status translation
status_translate = list('geen storing' = '0', 'storing' = 1, 'in bedrijf' = '1', 'uit bedrijf' = '0', 'dicht' = '1', 'niet dicht' = 0, 'open' = 1, 'niet open' = 0)

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

	load(paste0(conv_folder, '/', item))

	df_meta <- meta %>% subset(str_replace(id, '.csv', '.RData') == item)

	df %<>% subset(time >= time_0 & time <= time_1)
	df %<>% mutate(id = item)
	df %<>% mutate(time = round_date(time, unit = time_rounding))

	if(df_meta$class %in% c("flow", "pressure", "conductivity", "temperature", "acidity", "turbidity", '-')) {
		df %<>% mutate(value = as.numeric(value)) %<>% na.omit %<>% data.table
		df <- df[,list(value = mean(value, na.rm = T)), by = 'id,time']
	}

	if(df_meta$class %in% c("vitnor")) {
		df %<>% mutate(value = as.numeric(value)) %<>% na.omit
		df <- df[,list(value = max(value, na.rm = T)), by = 'id,time']
	}

	if(df_meta$class %in% c("status")) {
		df %<>% rowwise() %<>% mutate(value = as.character(status_translate[value]))
		df %<>% mutate(value = as.numeric(value)) %<>% na.omit %<>% data.table
		df <- df[,list(value = mean(value, na.rm = T)), by = 'id,time']
	}

	return(df)

}

# Share settings with every cluster worker
clusterExport(cl, list('conv_folder', 'meta', 'work', 'aggr_folder', 'time_0', 'time_1', 'status_translate', 'time_rounding'))

##############################
########## Execute work ######
##############################

# Apply parallel
items <- dir(conv_folder, cache_filename_pattern)
results <- parLapplyLB(cl, items, function(item){
	print(item)

	result <- tryCatch({
		aggr <- paste0(aggr_folder, '/', str_replace(item, '[.]RData', '.RData'))
		if(!file.exists(aggr)) {
			df <- work(item)
			save(df, file = aggr)
		}
		return(list(item = item, result = 'ok', error = ''))
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

results %<>% rbindlist
