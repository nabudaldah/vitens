
##############################
########## Settings ##########
##############################

##############################
########## Setup    ##########
##############################

# Load libraries
source('./0.setup.R')

# Load meta-data
load(meta_file)
load(data_table_file)

##############################
########## Cluster ###########
##############################

# Startup parallel workers
cores <- detectCores() - 1
cl <- makeCluster(cores, outfile = log_file)
clusterEvalQ(cl, source('./0.setup.R'))

##############################
########## Work function #####
##############################

data_matrix <- data_table
data_matrix$time <- NULL
column_names_original <- colnames(data_matrix)
variable_count <- ncol(data_matrix)
colnames(data_matrix) <- paste0('x', 1:variable_count)
column_names <- colnames(data_matrix)
data_matrix[is.na(data_matrix)] <- 0

formulas <- sapply(1:variable_count, function(current_column){
	f <- paste0('x', current_column, ' ~ ', paste0(column_names[-current_column], collapse = ' + '))
	return(as.formula(f))
})

# Share settings with every cluster worker
clusterExport(cl, list('temp_folder', 'conv_folder', 'meta', 'aggr_folder', 'data_matrix'))

models <- parLapply(cl, formulas, function(f){
	print(f)
	m <- lm(f, data_matrix)
	return(m)
})


# Work to be done per item
work <- function(item){

	return(df)
}

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

