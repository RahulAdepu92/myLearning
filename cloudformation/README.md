# CloudFormation Templates

This folder contains the CloudFormation Templates (CFT) used to deploy our code to AWS.

* `glue.yml` - for deploying Glue jobs
* 'lambda_s3_trigger_step1.yml', 'lambda_s3_trigger_step2.yml', 'lambda_s3_trigger_step3.yml' - for deploying Lambda functions with triggers;follow below steps for delpoyment

1. create "myLambdaStack" for deploying required number of lambdas with "Type: AWS::Lambda::Permission" and "Type: 'AWS::Lambda::Function'" resources;
   at this satge "Type: AWS::S3::Bucket" resource should be absent in "myLambdaStack";
   update "myLambdaStack" using command-
   aws --profile ahub_devx cloudformation deploy --template .\lambda_s3_trigger_step1.yml --stack-name irx-accumhub-lambda-s3-trigger
   
2. add "Type: AWS::S3::Bucket" resource to the above "myLambdaStack";
   update "myLambdaStack" using command-
   aws --profile ahub_devx cloudformation deploy --template .\lambda_s3_trigger_step2.yml --stack-name irx-accumhub-lambda-s3-trigger;
   this is a one time activity and is run for the first delpoyment in any environment, if the bucket is already present and is created out of cft;
   if this step is already executed before and you are making just changes to your "myLambdaStack"(like adding lambdas), then directly go to next step skipping this, else an error "bucket already exists" is thrown
   
3. create and import change set "Type: AWS::S3::Bucket" into "myLambdaStack" through console or AWS CLI-refer below link for steps to follow https://urldefense.proofpoint.com/v2/url?u=https-3A__aws.amazon.com_premiumsupport_knowledge-2Dcenter_cloudformation-2Ds3-2Dnotification-2Dlambda_&d=DwICaQ&c=A-GX6P9ovB1qTBp7iQve2Q&r=YGDYeBqpTHCYR7B9acbZpZrxdJlcC6jDqRw0hBaUn-E&m=VWNk2ApmVVHb98lzV1UYwmrhmuH1-IoJtprXpTLEgCA&s=Iv18q43xHLA_0Z4xmGdzf65ilfzuyvxtS-vUCWo9fek&e=;
   
4. lastly add NotificationConfiguration to "Type: AWS::S3::Bucket" resource in "myLambdaStack";
   update "myLambdaStack" using command-
   aws --profile ahub_devx cloudformation deploy --template .\lambda_s3_trigger_step3.yml --stack-name irx-accumhub-lambda-s3-trigger;