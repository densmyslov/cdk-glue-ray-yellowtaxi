from aws_cdk import (
                        Stack,
                        aws_s3 as s3,
                        RemovalPolicy,
                        aws_iam as iam,
                        CfnOutput,
                        aws_glue as glue,
                        aws_s3_assets as s3_assets
                    )
from constructs import Construct

class CdkGlueRayYellowtaxiStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get the environment name and configuration from cdk.json
        env_name = self.node.try_get_context("env_name") or "stage"
        env_config = self.node.try_get_context("env")[env_name]

#################################################################################
# Generate S3 Bucket (Step 1 of Tutorial)
################################################################################
        # get bucket_name depending on environment
        bucket_name = env_config.get("bucket_name")
        if not bucket_name:
            raise ValueError(f"bucket_name is not defined for environment: {env_name}")
        
        # Set RemovalPolicy based on environment
        if env_name == "stage":
            removal_policy = RemovalPolicy.DESTROY
        else:
            removal_policy = RemovalPolicy.RETAIN
        
        # Create the S3 bucket
        s3.Bucket(self, "MyBucket",
            bucket_name=bucket_name,
            removal_policy=removal_policy,
            auto_delete_objects=True 
        )


#########################################################################################
# IAM Role for Glue (Step 2 of Tutorial)
#########################################################################################


        # Create an IAM role for Glue with Ray
        glue_ray_role = iam.Role(
            self, "GlueRayJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Role for Glue job running Ray",
        )

        # Attach managed policies needed by Glue
        glue_ray_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )

        glue_ray_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:PutObject",
            ],
            resources=[f"arn:aws:s3:::{bucket_name}/*"]
        ))



        # Output the role ARN for reference
        CfnOutput(self, "GlueRayRoleArn", value=glue_ray_role.role_arn, description="The ARN of the Glue Ray job IAM Role")

#########################################################################################
# Glue Layer (Step 4)
#########################################################################################
        # Define the psutil layer from the local ZIP file
        psutil_layer_asset = s3_assets.Asset(self, "PsutilLayerAsset",
            path="assets/psutil_layer.zip"  # Reference the local ZIP file
        )
        

        CfnOutput(self, "PsutilLayerS3Url", 
            value=psutil_layer_asset.s3_object_url, 
            description="S3 URL of the psutil layer asset"
        )




#########################################################################################
# Glue Job (Step 3)
#########################################################################################
        # Upload the Glue job script to S3
        glue_script_asset = s3_assets.Asset(self, "GlueRayJobScript",
            path="scripts/glue_job.py"
        )

#########################################################################################
# Add permissions for glue_ray_role to access the Glue script and psutil layer in S3
#########################################################################################
        # Add permissions for Glue job to access the Glue script and psutil layer in S3
        glue_ray_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
            ],
            resources=[
                f"arn:aws:s3:::{glue_script_asset.s3_bucket_name}/{glue_script_asset.s3_object_key}",
                f"arn:aws:s3:::{psutil_layer_asset.s3_bucket_name}/{psutil_layer_asset.s3_object_key}" 
            ]
        ))



        
        # Create a Glue Job
        glue.CfnJob(
            self, "GluePythonShellJob",
            role=glue_ray_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="pythonshell",
                python_version="3.9",
                script_location=glue_script_asset.s3_object_url
            ),
            glue_version="3.0",
            max_capacity=1,
            description="AWS Glue job for processing data using Python Shell",
            default_arguments={
                "--ENV_NAME": env_name,
                "--BUCKET_NAME": bucket_name,
                "--extra-py-files": psutil_layer_asset.s3_object_url
            }
        )


