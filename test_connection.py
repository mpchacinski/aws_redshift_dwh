import boto3
import configparser
import psycopg2

dwh_config = configparser.ConfigParser()
aws_config = configparser.ConfigParser()

dwh_config.read_file(open('dwh.cfg'))
aws_config.read_file(open('aws_auth.cfg'))

KEY                    = aws_config.get('AWS','KEY')
SECRET                 = aws_config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = dwh_config.get('DWH','DWH_CLUSTER_IDENTIFIER')
DWH_PORT               = dwh_config.get("CLUSTER","DB_PORT")

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*dwh_config['CLUSTER'].values()))
cur = conn.cursor()

cur.execute("SELECT CURRENT_DATE")
print(cur.fetchall())

conn.close()