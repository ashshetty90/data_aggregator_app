# Q1) What happens in the database server under the hood when one executes a query?
* Assuming that the query is submitted to a POSTGRES sql server,
the first step in the process is the parsing of the text sql query.
* In this process the SQL text received from the client application is scanned to check for syntax errors. 
* Once its parsed successfully, the parsed tree is passed on to the query planner. 
* The planner is responsible for finding all possible plans for executing a query.
* The plan can be a sequential or index based plan depending on whether indexes have been identified. The execution plans are developed in terms of the query operators.
* When all possible execution plans of the query are generated, the optimiser searches for the least expensive plan based on estimates measured in Disk and I/O
* After choosing the least-expensive execution plan, the query executor starts at the beginning of the plan and asks the topmost operator to produce a result set.
* Each operator transforms its input set into a result set, the input set may come from another operator lower in the tree.
* When the topmost operator completes its transformation, the results are returned to the client application.

# Q2) What would a database engine consider to execute your query effectively?
* Query Complexity in terms of the joins used, the filters added and the aggregations done.
* Is there any lock on the table being accessed. This largely impacts performance if a lot of users are accessing the same table
* How the data is duplicated or sharded across the cluster. The number of replicas etc
* Are there any indexes that can be leveraged to query the data faster
* Is there any relatively small table that can be loaded into memory for quick access
* Based on the type of the database, it also depends if the workload is analytical or transactional. 
  * When dealing with analytical data(OLAP), columnar data formats are more performant
  * When dealing with transactional data(OLTP), row based data formats are more performant
