import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk_glue_ray_yellowtaxi.cdk_glue_ray_yellowtaxi_stack import CdkGlueRayYellowtaxiStack

# example tests. To run these tests, uncomment this file along with the example
# resource in cdk_glue_ray_yellowtaxi/cdk_glue_ray_yellowtaxi_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CdkGlueRayYellowtaxiStack(app, "cdk-glue-ray-yellowtaxi")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
