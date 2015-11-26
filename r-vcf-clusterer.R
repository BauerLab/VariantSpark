library(biganalytics)

f1 ="/Users/obr17q/Documents/workspace/genespark/data/data2.vcf"

### Converts a variant to a numeric, i.e. "1|1:12" to 2 (1+1)
hamming <- function(a){
  a <- substr(a, 0, 3)
  a <- strsplit(a, "|", fixed = TRUE)
  a <- vapply(a, function(x) sum(as.numeric(x)), as.numeric(1))
}

### With VariantAnnotation
vcf1 <- as.data.frame(readGT(f1), stringsAsFactors=FALSE) #Read VCF file
vcf1 <- sapply(vcf1, hamming) #Apply Hamming function
vcf1 <- t(vcf1) #Transpose matrix

### Without VariantAnnotation
vcf2 <- read.table(f1, sep="\t",as.is=TRUE) #Read VCF file
vcf2[1:9] <- list() #Remove the first 9 columns (these aren't variants)
vcf2 <- sapply(vcf2, hamming) #Apply Hamming function
vcf2 <- t(vcf2) #Transpose matrix

### Do the two methods return the same matrix?
all(vcf1[1] == vcf2[1])

### Create big.matrix as required by bigkmeans
x <- as.big.matrix(vcf1)

### Build a kmeans model
ans <- bigkmeans(x, 2, nstart=20)

### Calculate within set sum of squared error
wss[i] <- sum(bigkmeans(x, centers=3)$withinss)
rand
