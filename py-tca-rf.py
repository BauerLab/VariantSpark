import csv
import numpy as np
import scipy.sparse
import sklearn.ensemble

rows = np.array([],dtype=int)
cols = np.array([],dtype=int)
vals = np.array([],dtype=int)

llabels = []

header = None 
irow = 0
icol = 0
with open('data/tcga_csv_1k.csv') as f:
	reader = csv.reader(f)
    	for row in reader:
		if header:
			llabels.append(int(float(row[1])))	
			data = map(lambda s:int(float(s)),row[2:])		
        		vals = np.append(vals, filter(lambda v: v != 0, data))	
        		rows = np.append(rows, map(lambda v:irow,filter(lambda v: v != 0, data)))	
			cols = np.append(cols, map(lambda (i,v):i, filter(lambda (i,v): v !=0, enumerate(data))))
			irow += 1
		else:
			header = row
			icol = len(row) - 2
		
mtx = scipy.sparse.csr_matrix((vals,(rows,cols)), shape=(irow,icol))
print(mtx)
labels = np.array(llabels,dtype=int)
print(labels)

rf = sklearn.ensemble.RandomForestClassifier(n_estimators=500, oob_score=True,verbose=1, n_jobs=4)
rf.fit(mtx.toarray(),labels)
print(rf.oob_score_)
print(rf.feature_importances_)

