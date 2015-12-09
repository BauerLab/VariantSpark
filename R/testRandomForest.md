Run rf on a small saple of genom
========================================================

Run rf on a small saple of genom


```r
library(randomForest)
```

```
## randomForest 4.6-10
## Type rfNews() to see new features/changes/bug fixes.
```

```r
data <-read.table('wide-data.csv',header=FALSE, sep=",")

X <- t(data[-1,])
y <- as.factor(t(data[1,]))

model <-randomForest(X,y,ntree=100, importance=TRUE)
```

Plot OOB error rates


```r
plot(model)
```

![plot of chunk unnamed-chunk-2](figure/unnamed-chunk-2-1.png) 


```r
model$err.rate[90:100,]
```

```
##             OOB          0         1          2          3
##  [1,] 0.1810261 0.03260870 0.8372093 0.04072398 0.06868132
##  [2,] 0.1781220 0.03260870 0.8197674 0.04072398 0.06868132
##  [3,] 0.1800581 0.03623188 0.8197674 0.04072398 0.07142857
##  [4,] 0.1781220 0.03623188 0.8197674 0.04072398 0.06593407
##  [5,] 0.1800581 0.03623188 0.8255814 0.04072398 0.06868132
##  [6,] 0.1829622 0.03623188 0.8430233 0.04072398 0.06868132
##  [7,] 0.1761859 0.03260870 0.8255814 0.04072398 0.06043956
##  [8,] 0.1790900 0.03623188 0.8313953 0.04072398 0.06318681
##  [9,] 0.1790900 0.03623188 0.8372093 0.04072398 0.06043956
## [10,] 0.1819942 0.03623188 0.8488372 0.04072398 0.06318681
## [11,] 0.1819942 0.03623188 0.8430233 0.04072398 0.06593407
```




Plot variable importance


```r
varImpPlot(model)
```

![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-4-1.png) 


