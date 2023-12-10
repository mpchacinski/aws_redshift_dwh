import boto3
import configparser

dwh_config = configparser.ConfigParser()
aws_config = configparser.ConfigParser()

dwh_config.read_file(open('dwh.cfg'))
aws_config.read_file(open('aws_auth.cfg'))

KEY                    = aws_config.get('AWS','KEY')
SECRET                 = aws_config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = dwh_config.get('DWH','DWH_CLUSTER_IDENTIFIER')
DWH_IAM_ROLE_NAME      = dwh_config.get("DWH", "DWH_IAM_ROLE_NAME")

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

iam = boto3.client("iam",aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name="us-east-1"
                  )

redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

cluster_spec = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
cluster_status = cluster_spec["ClusterStatus"]
print(cluster_status)

iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)