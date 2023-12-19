"""Delete Redshift cluster and role associated with DWH User."""

import boto3
import configparser

dwh_config = configparser.ConfigParser()
aws_config = configparser.ConfigParser()

dwh_config.read_file(open('dwh.cfg'))
aws_config.read_file(open('aws_auth.cfg'))

KEY = aws_config.get('AWS','KEY')
SECRET = aws_config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = dwh_config.get('DWH','dwh_cluster_identifier')
DWH_IAM_ROLE_NAME = dwh_config.get("DWH", "dwh_iam_role_name")

redshift = boto3.client("redshift",
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

iam = boto3.client("iam",aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name="us-east-1"
                  )

print("Deleting Redshift Cluster...")
redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                        SkipFinalClusterSnapshot=True)

cluster_spec = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
cluster_status = cluster_spec["ClusterStatus"]
print(f"Cluster status: {cluster_status}")

print("Deleting IAM role...")
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
