
##############################
########## Setup #############
##############################

# Setup
source('./0.setup.R')

load(meta_file)
load(data_table_file)

nolegend <- theme(legend.position="none")

data_isna <- data_table
data_isna$time <- NULL
data_isna %<>% as.matrix
data_isna[!is.na(data_isna)] <- 1
data_isna[ is.na(data_isna)] <- 0

image(data_isna)

data_isna %<>% colSums
data_isna <- data.frame(id = names(data_isna), n = data_isna)
data_isna %<>% arrange(n)
data_isna %<>% mutate(r = 1:nrow(.))

png(paste0('analysis/',format(Sys.time(), '%Y-%m-%d %H%M'),' missing data.png'), width = 800, height = 600)
data_isna %>% ggplot() + aes(r / nrow(.), n / max(n)) + geom_point() + labs(x = 'Percent of data', y = 'Percent containing values')
dev.off()

# LM

x <- data_table
x$time <- NULL
variable_count <- ncol(x)
colnames(x) <- paste0('x', 1:variable_count)
column_names <- colnames(x)
x[is.na(x)] <- 0

models <- lapplyPar(1:variable_count, function(current_column){
	f <- paste0('x', current_column, ' ~ ', paste0(column_names[-current_column], collapse = ' + '))
	f %<>% as.formula
	m <- lm(f, x)
	return(m)
})


summary(m)

apply(x, 2, is.na) %>% length

x %<>% as.data.table
x %<>% melt

nas <- x %>% group_by(variable) %>% summarise(nas = sum(is.na(value)))

plot(nas$variable, nas$nas / nrow(data_table))

##############################

corr_file     <- paste0(temp_folder, '/corr.RData')
load(corr_file)
corr <- data_corr_list %>% dcast(x ~ y)
corr$x <- NULL
corr_names <- colnames(corr)
rownames(corr) <- corr_names
corr %<>% as.matrix
corr[is.na(corr)] <- 0
heatmap(corr, na.rm = T, keep.dendro = F, )

sc <- read.table('E:/Rtmp/synthetic_control.data', header=F, sep='')

sc <- data_table
sc[is.na(sc)] <- 0
sc$time <- NULL
# randomly sampled n cases from each class, to make it easy for plotting
n <- 10
s <- sample(1:100, n)
idx <- c(s, 10+s, 20+s, 30+s, 40+s, 50+s)
sample2 <- sc[1:10,1:1000]

# compute DTW distances
library(dtw)
distMatrix <- dist(sample2, method='DTW')

# hierarchical clustering
hc <- hclust(distMatrix, method='average')
plot(hc)

dist.matrix <- as.matrix()

dist()
heatmap()

##############################
########## Vitnor items ######
##############################

items <- meta %>%
	subset(str_detect(id, 'vitnor')) %>% select(id) %>%
	mutate(id = str_replace(id, '.csv', '.RData')) %>% unlist

data <- lapply(items, function(item) {
	filename <- paste0(conv_folder, '/', item)
	if(file.exists(filename))	{
		load(filename);
		df %<>% subset(time > as.POSIXct('2015-03-01') & time < as.POSIXct('2016-05-01'))
		df %<>% mutate(value = as.numeric(value))
		df %<>% mutate(hour = round_date(time, unit = 'hour'))
		df %<>% na.omit

		df %<>% mutate(id = item)
		return(df)
	} else {
		return(data.frame())
	}
}) %>% rbindlist

data_subset <- data
data_subset %<>% subset(time > as.POSIXct('2015-03-01') & time < as.POSIXct('2016-05-01'))
data_subset %<>% mutate(value = as.numeric(value))
data_subset %<>% mutate(hour = round_date(time, unit = 'hour'))
data_subset %<>% na.omit

data_aggr <- data_subset %>% group_by(id, hour) %>% summarise(value = max(value))

#data_aggr %>% ggplot() + aes(hour, value, color = id) + geom_point() + nolegend
data_aggr %>% ggplot() + aes(hour, value, color = id) + geom_point() + nolegend




