#!/usr/bin/env python3
import os
from aws_cdk import App, Environment

from cdk_glue_ray_yellowtaxi.cdk_glue_ray_yellowtaxi_stack import CdkGlueRayYellowtaxiStack


prod_env=Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), 
                             region=os.getenv('CDK_DEFAULT_REGION'))

stage_env=Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), 
                             region='us-east-2')
app = App()
CdkGlueRayYellowtaxiStack(app, "CdkGlueRayYellowtaxiStack-Stage", env=stage_env)
CdkGlueRayYellowtaxiStack(app, "CdkGlueRayYellowtaxiStack-Prod", env=prod_env)


# Synthesize the app
app.synth()
