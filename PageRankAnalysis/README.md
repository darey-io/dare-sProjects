Tasks 1. Load PageRank dataset into an RDD 
2. Create a Links RDD with all outgoing links from a page 
3. initialize a ranks RDD with all ranks = 1 
4. Join the links and ranks RDDs 
5. Each node transfers its rank equally to its neighbors 
6. Apply a reduce operation on thw RDD to sum up values for the same node 
7. Apply the damping factor and use these as the updated ranks RDD 8. Repeat steps 4-7