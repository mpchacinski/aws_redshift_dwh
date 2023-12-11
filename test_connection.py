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
LOG_JSON_PATH = dwh_config.get("S3", "log_jsonpath")

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

cluster_spec = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)["Clusters"][0]

DWH_ENDPOINT = cluster_spec["Endpoint"]["Address"]

# dwh_config.set("CLUSTER", "HOST", DWH_ENDPOINT)
#
# with open("dwh.cfg", "w") as configfile:
#     dwh_config.write(configfile)

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*dwh_config["CLUSTER"].values()))
cur = conn.cursor()

staging_events_table_create= ("""
DROP TABLE IF EXISTS staging_events;
 
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar(150),
    auth varchar(20),
    first_name varchar(300),
    gender varchar(1),
    item_in_session integer,
    last_name varchar(30),
    length numeric(10,4),
    level varchar(10),
    location varchar(150),
    method varchar(10),
    page varchar(20),
    registration varchar(50),
    session_id integer,
    song varchar(200),
    status varchar(4),
    ts bigint,
    user_agent varchar(150),
    user_id integer
);
""")

cur.execute(staging_events_table_create)
conn.commit()

SQL_COPY = """
copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
format as json {} region 'us-west-2';
        """.format(DWH_ROLE_ARN, LOG_JSON_PATH)

cur.execute(SQL_COPY)
conn.commit()

conn.close()