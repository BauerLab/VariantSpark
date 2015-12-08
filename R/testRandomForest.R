library(randomForest)
data <-read.table('wide-data.csv',header=FALSE, sep=",")

X <- t(data[-1,])
y <- as.factor(t(data[1,]))

model <-randomForest(X,y,ntree=500, importance=TRUE)