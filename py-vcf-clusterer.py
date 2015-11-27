import pandas as pd
import io
file_path = "/Users/obr17q/Documents/workspace/genespark/data/data2.vcf"

### Convets strings from a serial to integers
def hamming( variant ):
    return variant\
    .str.split(':')\
    .str.get(0)\
    .str.split('|')\
    .apply(lambda x: sum(map(int, x)))

### Read in the VCF file and drop first 9 columns
with io.open(file_path, 'r') as f:
    for line in f:
        if line[0:2] == '##':
            pass
        else:
            break
    vcf = pd.read_csv(f, sep='\t', header=None)
    vcf = vcf.drop(vcf.columns[[range(0,9)]], axis=1)

### Apply Hamming to DataFrame and transpose
vcf = vcf.apply(hamming).T

