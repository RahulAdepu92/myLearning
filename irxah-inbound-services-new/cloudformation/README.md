# CloudFormation Templates

This folder contains the CloudFormation Templates (CFT) used to deploy our code to AWS.

* 'glue.yml' - YAML template for deploying Glue jobs
* 'lambda_s3_trigger_step1.yml', 'lambda_s3_trigger_step2.yml', 'lambda_s3_trigger_step3.yml' - YAML template for deploying Lambda functions with triggers;follow below steps for delpoyment
* 'sns.yml' - YAML template for deploying the predefined SNS topics and endpoint(email) 
* 'redshift-cluster.yml' - YAML template for deploying new redshift cluster in desired AWS region
* 'deploy.ps1' - powershell script that contains cli cft commands at one place and is designed to run in any environment using variables in settings.json. It can
    1. deploys glue jobs
    2. creates lambda version in defined lambda layer
    3. deploys lambda functions along with s3 triggers addition
    4. deploys sns topics
* 'deploy-redshift.ps1' - powershell script that contains cli cft command only to deploy redshift cluster. This has been isolated from origina deploy.ps1 because cluster creation is a one time activity and is usually done in DR regions. This will be not used in primary regions.
* 'settings.json' - Json script that has all environment variables(values & keys) which will be retrieved and used by deploy.ps1 script during deployment.  

Summary:
1. one and only one command used to deploy whole lambda functions and jobs:
    .\deploy.ps1 <environment>
2. command used to deploy redshift cluster:
    .\deploy-redshift.ps1 <environment>
    
# environment can accept dev/sit/uat/prod/uat_dr/prod_dr