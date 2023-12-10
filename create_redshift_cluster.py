import boto3
import configparser
import json

dwh_config = configparser.ConfigParser()
aws_config = configparser.ConfigParser()

dwh_config.read_file(open('dwh.cfg'))
aws_config.read_file(open('aws_auth.cfg'))


KEY                    = aws_config.get('AWS','KEY')
SECRET                 = aws_config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = dwh_config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = dwh_config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = dwh_config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = dwh_config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = dwh_config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = dwh_config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = dwh_config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = dwh_config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = dwh_config.get("DWH", "DWH_IAM_ROLE_NAME")

# Creating clients for iam and redshift
iam = boto3.client("iam",aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name="us-east-1"
                  )

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Creating a role
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

cluster_spec = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)["Clusters"][0]

DWH_ENDPOINT = cluster_spec["Endpoint"]["Address"]

dwh_config.set("CLUSTER", "HOST", DWH_ENDPOINT)
dwh_config.set("IAM_ROLE", "ARN", role_arn)

with open("dwh.cfg", "w") as configfile:
    dwh_config.write(configfile)

