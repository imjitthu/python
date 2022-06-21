import boto3
import pandas as pd
client = boto3.client('s3')
resp = client.select_object_content(
    Bucket = 'gpipis-iris-dataset',
    Key = 'iris.csv',
    Expression = """select * from S3Object s where s.variety='Setosa'""",
    ExpressionType = 'SQL',
    InputSerialization = {'CSV': {'FileHeaderInfo': 'Use'}},
    OutputSerialization = {'CSV': {}}
)
# create an empty file
f = open("myfile.txt","w")
f.close()
# read each record and append it to "myfile"
for event in resp['Payload']:
    if 'Records' in event:
        tmp = event['Records']['Payload'].decode()
        file1 = open("myfile.txt","a")
        file1.write(tmp)
        print(event['Records']['Payload'].decode())
file1.close()
# read the "myfile.txt" with pandas in order to confirm that it works as expected
df = pd.read_csv("myfile.txt", header=None)
print(df)