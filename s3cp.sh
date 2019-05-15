echo uploading data to S3 Bucket: $BUCKET
echo using local AWS profile: $PROFILE
aws s3 cp --recursive ./data s3://$BUCKET --profile $PROFILE