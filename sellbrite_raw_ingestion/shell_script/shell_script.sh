


/usr/lib/spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.18 --py-files s3://aws-poc-serverless-analytics/emr/jdbc_ingetion/sellbrite_raw_ingestion/raw_ingestion.zip --master yarn s3://aws-poc-serverless-analytics/emr/jdbc_ingetion/sellbrite_raw_ingestion/rawFromjdbc.py c.fnfd5bptoazdphlg6wknuvzcwse.db.citusdata.com s3://aws-poc-serverless-analytics/emr/jdbc_ingetion/sellbrite_ingestion/ s3://aws-poc-serverless-analytics/emr/jdbc_ingetion/tables_import/tables_hosts.json

