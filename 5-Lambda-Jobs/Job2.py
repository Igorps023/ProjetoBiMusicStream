import json
import boto3

def lambda_handler(event, context):
        # Initialize the EMR client
    connection = boto3.client(
        'emr',
        region_name='us-east-1',
        aws_access_key_id='SUA-CHAVE',
        aws_secret_access_key='SUA-CHAVE',
    )

    # Get a list of active clusters
    response = connection.list_clusters(ClusterStates=['STARTING', 'RUNNING', 'WAITING'])

    # Define the target cluster name
    target_cluster_name = 'Cluster Modules'

    # Search for the cluster by name in the list of active clusters
    cluster_id = None
    for cluster in response.get('Clusters', []):
        if cluster['Name'] == target_cluster_name:
            cluster_id = cluster['Id']
            break


    #If statement job and print execution when ok or error
    if cluster_id:
        print(f"Found cluster '{target_cluster_name}' with ID:", cluster_id)
        
        # Specify the path to your Python script on S3
        script_s3_path = 's3://PATHWAYTOPYFILE/yourfile2.py'
        
        step = {
            'Name': 'py-spark-step2',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode', 'cluster', script_s3_path]
            }
        }
        
        action = connection.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
        print("Added step:", action)
        
    else:
        print(f"No cluster named '{target_cluster_name}' found.")

    #End