import boto3
import configparser
import psycopg2

dwh_config = configparser.ConfigParser()
aws_config = configparser.ConfigParser()

dwh_config.read_file(open('dwh.cfg'))
aws_config.read_file(open('aws_auth.cfg'))

KEY = aws_config.get('AWS','KEY')
SECRET = aws_config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = dwh_config.get('DWH','DWH_CLUSTER_IDENTIFIER')
DWH_ROLE_ARN = dwh_config.get('IAM_ROLE','arn')

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

cluster_spec = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)["Clusters"][0]

DWH_ENDPOINT = cluster_spec["Endpoint"]["Address"]

dwh_config.set("CLUSTER", "HOST", DWH_ENDPOINT)

with open("dwh.cfg", "w") as configfile:
    dwh_config.write(configfile)

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*dwh_config["CLUSTER"].values()))
cur = conn.cursor()

staging_events_table_create= ("""
DROP TABLE IF EXISTS staging_events;
 
CREATE TABLE IF NOT EXISTS staging_events (
  artist varchar,
  auth varchar(22),
  gender varchar(1),
  item_in_session integer,
  last_name varchar(100),
  length varchar(20),
  level varchar(10),
  location varchar(100),
  method varchar(10),
  page varchar(20),
  registration varchar(50),
  session_id varchar(10),
  song varchar,
  status varchar(4),
  ts varchar(15),
  user_agent varchar(100),
  user_id varchar(10)
);
""")

cur.execute(staging_events_table_create)
conn.commit()

SQL_COPY = """
copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
        """.format(DWH_ROLE_ARN)

cur.execute(SQL_COPY)
conn.commit()

conn.close()