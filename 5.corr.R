
source('./0.setup.R')

load(data_table_file)

data_matrix <- data_table
data_matrix$time <- NULL
data_matrix <- as.matrix(data_matrix)


#data_corr[is.na(data_corr)] <- 0
data_corr <- cor(data_matrix, use = 'pairwise.complete.obs')
#data_corr[is.na(data_corr)] <- 0

# Plot heatmap (only limited number of correlations ...)
png(paste0('analysis/',format(Sys.time(), '%Y-%m-%d %H%M'),' vitnor heatmap.png'), width = 800, height = 600)
#data_corr_na <- data_corr[1:100,1:100]
data_corr_na <- data_corr
data_corr_na[is.na(data_corr_na)] <- 0
heatmap(data_corr_na, Rowv = F, Colv = F, na.rm = T, symm = T, labRow = F, labCol = F, keep.dendro = F, verbose = F)
dev.off()

data_corr_list <- melt(data_corr)
colnames(data_corr_list) <- c('x', 'y', 'c')

corr_file     <- paste0(temp_folder, '/corr.RData')
save(data_corr_list, file = corr_file)
corr_file_csv <- paste0(corr_file, '.csv')
write.table(data_corr_list, corr_file_csv, sep = ';', row.names = F, quote = F)




##### DELTA

data_matrix_0 <- data_matrix
data_matrix_1 <- data_matrix

data_matrix_0 %<>% rbind(   rep(NA, ncol(data_matrix_0)), .)
data_matrix_1 %<>% rbind(., rep(NA, ncol(data_matrix_1)))

dim(data_matrix_0) == dim(data_matrix_1)

data_matrix_d <- data_matrix_0 - data_matrix_1

dim(data_matrix)
dim(data_matrix_0)
dim(data_matrix_1)
dim(data_matrix_d)




#data_corr[is.na(data_corr)] <- 0
data_corr_d <- cor(data_matrix_d, use = 'pairwise.complete.obs')
#data_corr[is.na(data_corr)] <- 0

# Plot heatmap (only limited number of correlations ...)
png(paste0('analysis/',format(Sys.time(), '%Y-%m-%d %H%M'),' heatmap_d.png'), width = 800, height = 600)
#data_corr_na <- data_corr[1:100,1:100]
data_corr_na_d <- data_corr_d
data_corr_na_d[is.na(data_corr_na_d)] <- 0
heatmap(data_corr_na_d, Rowv = F, Colv = F, na.rm = T, symm = T, labRow = F, labCol = F, keep.dendro = F, verbose = F)
dev.off()

data_corr_list_d <- melt(data_corr_d)
colnames(data_corr_list_d) <- c('x', 'y', 'c')

corr_file_d     <- paste0(temp_folder, '/corr_d.RData')
save(data_corr_list_d, file = corr_file_d)
corr_file_csv_d <- paste0(corr_file_d, '.csv')
write.table(data_corr_list_d, corr_file_csv_d, sep = ';', row.names = F, quote = F)
