import boto3
import pandas as pd

client = boto3.client('s3')
bucket_name = ""
key_name = "iris.csv"
query = "select * from S3Object s where s.variety='Setosa'"
file = "myfile.txt"

resp = client.select_object_content(
    Bucket = bucket_name,
    Key = key_name,
    Expression = query,
    ExpressionType = 'SQL',
    InputSerialization = {'CSV': {'FileHeaderInfo': 'Use'}},
    OutputSerialization = {'CSV': {}}
)

# create an empty file
f = open(file,"w")
f.close()

# read each record and append it to "myfile"
for event in resp['Payload']:
    if 'Records' in event:
        tmp = event['Records']['Payload'].decode()
        file1 = open(file,"a")
        file1.write(tmp)
        print(event['Records']['Payload'].decode())
file1.close()

# read the "myfile.txt" with pandas in order to confirm that it works as expected
df = pd.read_csv(file, header=None)
print(df)