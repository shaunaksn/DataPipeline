echo 'mysql-u root -p****** -h xxx.x.x.x < /home/pravin/sample/dim_policy.sql'

echo 'mysql-u root -p****** -h xxx.x.x.x < /home/pravin/sample/dim_product.sql'

echo  `aws configure set aws_access_key_id default_access_key`
echo  `aws configure set aws_secret_access_key default_secret_key`
echo  `aws configure set default.region us-east-1`

echo `aws s3 sync "/mnt/c/ProgramData/mysql/MySql server 8.0" s3://test-bucket-snowflake-lab/ --region "us-east-1"`
