mkdir ./sellbrite_raw_ingestion
cp -R ./pyspark_app ./sellbrite_raw_ingestion
cp -R ./python_app ./sellbrite_raw_ingestion
cd ./sellbrite_raw_ingestion && zip raw_ingestion -r .
cd ..
cp -R ./shell_script ./sellbrite_raw_ingestion
cp ./pyspark_app/rawFromjdbc.py ./sellbrite_raw_ingestion
rm -r ./sellbrite_raw_ingestion/pyspark_app
rm -r ./sellbrite_raw_ingestion/python_app


