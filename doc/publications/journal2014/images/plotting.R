library("ggplot2")
library("reshape2")
library(plyr)


table1 <- read.table("~/Documents/datahome/VariantSpark/doc/publications/journal2014/images/Table1.txt", header=TRUE, quote="\"")
table1$name<-factor(table1$name,levels=c("Python","R","Hadoop","Adam","ADMIXTURE","VariantSpark"))

pdf("~/Documents/datahome/VariantSpark/doc/publications/journal2014/images/Resources.pdf", width=5, height=4)
ggplot(table1, aes(x=factor(name), y=time, fill=task)) + geom_bar(stat="identity") +
  ylab("time in seconds") + xlab("method") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
dev.off()

table2 <- read.table("~/Documents/datahome/VariantSpark/doc/publications/journal2014/images/Table2.txt", header=TRUE, quote="\"")
%table2$totalMem=table2$memory*table2$executors
%result2 <- cbind(table2[1],melt(table2[,c(2,5,6)]))
result2 <- cbind(table2[1],melt(table2[,c(2,3,4,5)]))
result2$task<-factor(result2$task,levels=c("pre-processing","clustering"))

pdf("~/Documents/datahome/VariantSpark/doc/publications/journal2014/images/Scaling.pdf", width=5, height=4)
ggplot(result2, aes(x=variants, y=value)) + geom_line(aes(colour = variable)) +
  facet_grid(variable ~ task, scales = "free") +
  ylab("value") + xlab("number of variants (%)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
dev.off()


data=data.frame(samples=c(1,270, 1097,11000,70000), dates=c(2001,2005,2012,2015,2017))
ggplot(data, aes(x=dates, y=samples)) + geom_line()
  