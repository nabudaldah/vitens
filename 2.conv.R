
##############################
########## Settings ##########
##############################

data_filename_pattern <- 'csv'

##############################
########## Setup    ##########
##############################

# Load libraries
source('./0.setup.R')

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

	df <- fread(paste0(data_folder, '/', item), stringsAsFactors = F, colClasses = rep('character',2), data.table = TRUE)
	colnames(df) <- c('time', 'value')

	df %<>% mutate(time = fastPOSIXct(time))

	return(df)

}

# Share settings with every cluster worker
clusterExport(cl, list('data_folder', 'work', 'conv_folder'))

##############################
########## Execute work ######
##############################

# Apply parallel
items <- dir(data_folder, data_filename_pattern)
results <- parLapply(cl, items, function(item){
	print(item)

	result <- tryCatch({
		cache <- paste0(conv_folder, '/', str_replace(item, '[.]csv', '.RData'))
		if(!file.exists(cache)) {
			df <- work(item)
			save(df, file = cache)
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
