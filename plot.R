run1 <- readr::read_tsv("log-sf100-2024-08-21T18:54:24.268973+00:00.tsv")
run1$run <- '1.0.0'
run2 <- readr::read_tsv("log-sf100-2024-08-21T19:18:13.127807+00:00.tsv")
run2$run <- 'main-mr'
run3 <- readr::read_tsv("log-sf100-2024-08-21T19:12:27.119128+00:00.tsv")
run3$run <- 'main-1'
runs <- rbind(run1, run2, run3)

library(ggplot2)
pdf("plot.pdf", height=5, width=15)
ggplot(runs, aes(x=time_offset, y=cpu_percent, colour=run)) + geom_line()
dev.off()