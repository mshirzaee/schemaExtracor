# schemaExtracor
Extracting the schema of a parquet file on HDFS and creating a Hive table based on this schema.

To run, you should use the function CreateTable() and define the arguments below:

table_mode: Which is the type of table in Hive which can be 'external' or an empty string '' (means internal) 

table_name: Your desired name for the table in Hive

hdfs_host: List of NameNode HTTP host strings, for example 'localhost'

hdfs_port: List of NameNode HTTP port integers, port defaults to 50070 if left unspecified

path: Path to the directory of parquet files. Files must be on HDFS, but you don't need to add 'hdfs://' to the path. for example '/tmp/this_is_a_dir'

other_options: You can leave it as '' (an empty string) if there is no more option to add, but to add some options like Serdes, declare them in string.
