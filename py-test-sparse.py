import numpy as np
import scipy.sparse


row = np.array([0, 0, 1, 2, 2, 2])
col = np.array([0, 2, 2, 0, 1, 2])
data = np.array([1, 2, 3, 4, 5, 6])
mtx = scipy.sparse.csr_matrix((data, (row, col)), shape=(3, 3))


print(row)
print(len(row))
print(col)
print(len(col))
print(data)
print(len(data))
print(mtx)
