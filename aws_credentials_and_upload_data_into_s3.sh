$ touch aws_credentials_and_upload_data_into_s3.sh
$ vi aws_credentials_and_upload_data_into_s3.sh

echo  `aws configure set aws_access_key_id aws-access-id`
echo  `aws configure set aws_secret_access_key aws-secret-key`
echo  `aws configure set default.region aws-region`

echo `aws s3 sync "/mnt/file-path" s3://mini-project-s3-raw-bucket/ --region "aws-region"`
