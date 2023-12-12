import boto3
import configparser
import json
import time

dwh_config = configparser.ConfigParser()
aws_config = configparser.ConfigParser()

dwh_config.read_file(open('dwh.cfg'))
aws_config.read_file(open('aws_auth.cfg'))


KEY = aws_config.get('AWS','KEY')
SECRET = aws_config.get('AWS','SECRET')

DWH_CLUSTER_TYPE = dwh_config.get("DWH","dwh_cluster_type")
DWH_NUM_NODES = dwh_config.get("DWH","dwh_num_nodes")
DWH_NODE_TYPE = dwh_config.get("DWH","dwh_node_type")

DWH_CLUSTER_IDENTIFIER = dwh_config.get("DWH","dwh_cluster_identifier")
DWH_DB = dwh_config.get("CLUSTER","db_name")
DWH_DB_USER = dwh_config.get("CLUSTER","db_user")
DWH_DB_PASSWORD = dwh_config.get("CLUSTER","db_password")
DWH_PORT = dwh_config.get("CLUSTER","db_port")

DWH_IAM_ROLE_NAME = dwh_config.get("DWH", "dwh_iam_role_name")

# Creating clients for iam and redshift
iam = boto3.client("iam",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name="us-east-1"
                  )

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Creating a role
print("Creating a role to call AWS services")
try:
    dwh_role = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )
except Exception as e:
    print(e)

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                       )['ResponseMetadata']['HTTPStatusCode']

role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

# Creating Redshift Cluster
print("Creating a redshift cluster...")
try:
    response = redshift.create_cluster(
        # HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        # Roles (for s3 access)
        IamRoles=[role_arn]
    )
except Exception as e:
    print(e)

# Wait three mins for the cluster to initiate creating process in order to get its parameters
time.sleep(180)

cluster_spec = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)["Clusters"][0]

DWH_ENDPOINT = cluster_spec["Endpoint"]["Address"]

dwh_config.set("CLUSTER", "HOST", DWH_ENDPOINT)
dwh_config.set("IAM_ROLE", "ARN", role_arn)

with open("dwh.cfg", "w") as configfile:
    dwh_config.write(configfile)

