
##############################
########## Settings ##########
##############################

# ... no settings ...

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

	df <- read.table(paste0(data_folder, '/', item), comment.char = '', stringsAsFactors = F, colClasses = rep('character',1), sep = '\1', header = F, nrows = 8)
	colnames(df) <- c('line')
	df %<>% mutate(header  = str_sub(line,  1, 25))
	df %<>% mutate(content = str_sub(line, 26, 1000))
	df %<>% mutate(line = NULL)

	df <- as.data.frame(t(df[,2]))
	colnames(df) <- c('point', 'description1', 'description2', 'units', 'accuracy', 'stepped', 'timestamp', 'resolution')
	df %<>% mutate(id = item)
	return(df)

}

# Share settings with every cluster worker
clusterExport(cl, list('work', 'meta_folder', 'data_folder'))

##############################
########## Execute work ######
##############################

# Apply parallel
#meta <- data.frame(filename = sort(dir(data_folder, 'csv')))
#meta %<>% mutate(id = str_pad(1:nrow(meta), 3, 'left', '0'))
items <- sort(dir(data_folder, 'csv'))
results <- parLapply(cl, items, function(item){

	result <- tryCatch({
		cache <- paste0(meta_folder, '/', str_replace(item, '.csv', '.RData'))
		if(file.exists(cache)) {
			load(cache)
		} else {
			res <- work(item)
			save(res, file = cache)
		}
		return(list(item = item, result = res, error = NULL))
	}, error = function(err){
		return(list(item = item, result = NULL, error = err))
	})

	return(result)

})

# Shut down cluster workers (!)
tryCatch({ stopCluster(cl) }, error = warning)

##############################
########## Save results ######
##############################

# Build meta database and save to cache
meta <- results %>%
	Filter(function(result) is.null(result[['error']]), .) %>%
	Map(function(result) result[['result']], .) %>%
	Reduce(rbind, .)

# Build error list and save to cache
meta_errors <- results %>%
	Filter(function(result) !is.null(result[['error']]), .) %>%
	Map(function(result) result[['error']], .) %>%
	Reduce(rbind, .)

##############################
########## Classify ##########
##############################

meta %<>% mutate(class = '-')

pattern_status <- 'status dicht|status open|status storing|status in|status belast|status onbelast'
meta %<>% mutate(class = ifelse(str_detect(point, pattern_status), 'status', class))
meta %<>% mutate(class = ifelse(str_detect(point, 'FT|VO|levering'), 'flow', class))
meta %<>% mutate(class = ifelse(str_detect(point, 'vitnor'), 'vitnor', class))
meta %<>% mutate(class = ifelse(str_detect(point, 'temp|TM'), 'temperature', class))
meta %<>% mutate(class = ifelse(str_detect(point, 'PT|DO'), 'pressure', class))
meta %<>% mutate(class = ifelse(str_detect(point, 'GO01'), 'conductivity', class))

meta %<>% mutate(class = ifelse(str_detect(units, 'S/cm'), 'conductivity', class))
meta %<>% mutate(class = ifelse(str_detect(units, 'm3/h'), 'flow', class))
meta %<>% mutate(class = ifelse(str_detect(units, 'C$'), 'temperature', class))
meta %<>% mutate(class = ifelse(str_detect(units, 'kPa'), 'pressure', class))
meta %<>% mutate(class = ifelse(str_detect(units, 'ntu|NTU'), 'turbidity', class))
meta %<>% mutate(class = ifelse(str_detect(units, 'pH'), 'acidity', class))

meta %<>% mutate(class = ifelse(str_detect(description1, 'pomp|afsl|alarm'), 'status', class))

shares <- meta %>%
	group_by(class) %>%
	summarise(count = length(class), share = count / nrow(meta)) %>%
	arrange(desc(share))

save(meta, file = meta_file)
