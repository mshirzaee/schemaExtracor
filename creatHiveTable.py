from pyspark.sql import SQLContext, HiveContext
import pyhdfs


def SchemaExtractor(hdfs_host=HDFS_HOST, hdfs_port=HDFS_IPC_PORT, path=None):
    ''' Infer the schema of a Parquet file from its metadata

    :param hdfs_host:
    :param hdfs_port:
    :param path: path to the parquet file
    :return: schema (String)
    '''
    sqlContext = SQLContext(sc)
    seperator = ':'
    hdfs_path = [hdfs_host, str(hdfs_port)]
    fs = pyhdfs.HdfsClient(hosts=seperator.join(hdfs_path), user_name='spark24')

    file_list = []
    for root, _, filenames in fs.walk(path):
        for filename in filenames:
            if filename.endswith(".parquet"):
                file_list.append(root + '/' + filename)

    single_parquet_path = file_list[0]

    df = sqlContext.read.parquet('hdfs://' + hdfs_host + single_parquet_path)

    schema = ''
    for name, dtype in df.dtypes:
        schema = schema + ' ' + str(name) + ' ' + str(dtype) + ','
    schema = schema[:-1]
    return schema


def CreateTable(table_mode='external', table_name=None, hdfs_host=None, hdfs_port=None, path=None,
                other_options=''):
    ''' Create a hive table based on the schema inferred from SchemaExtractor

    :param table_mode: 'external' or '' (which encounters internal)
    :param table_name: String
    :param hdfs_host:
    :param hdfs_port:
    :param path: path to the parquet file
    :param other_options: including serds, etc.
    :return:
    '''
    schema = SchemaExtractor(hdfs_host=hdfs_host, hdfs_port=hdfs_port, path=path)
    table_location = 'hdfs://' + hdfs_host + path
    hiveContext = HiveContext(sc)
    hive_query = """create {table_mode} table {table_name}({schema}) {options} stored as parquet location '{location_path}'""".format(
        table_mode=table_mode, table_name=table_name, schema=schema, location_path=table_location,
        options=other_options)
    print(hive_query)
    hiveContext.sql(hive_query)
