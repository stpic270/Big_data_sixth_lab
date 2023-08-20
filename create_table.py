from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import os
import re
import cassandra
import time
import json

def executions(ses, q):
  flag=True
  while flag==True:
    try:
      ses.execute(q)
      flag = False
    except cassandra.OperationTimedOut as er:
      print(er)
      print(f'This time cassandra did not answerimplement the {q} querry, program will sleep for 10s and try again')
      time.sleep(10)

# Pattern to extract ip to cassandra
pattern =r'(\d+.\d+.\d+.\d+)/\d+'

# Extraction of credentials
def get_credentials():
  credentials = []
  l, p = 'login:', 'password:'
  with open('test/cassandra_config.txt', 'r') as f:
    for line in f:
      s = line.strip()
      if l in s:
        le = len(l)
        credentials.append(line.strip()[le:])
      if p in s:
        le = len(p)
        le2 = len(credentials[0])
        credentials.append(line.strip()[le:le+le2])
    f.close
  return credentials

credentials = get_credentials()

# Get ip
def get_ip(path):
  with open(path, 'r') as ip:
    for line in ip:
      if '172.' in line:
        sp = re.findall(pattern, line)
        return sp[0]

credentials.append(get_ip('test/cassandra_ip.txt'))
  

auth_provider = PlainTextAuthProvider(username=credentials[0], password=credentials[1])

# Connect to cassandra
flag=True
while flag==True:
  try:
    cluster = Cluster([credentials[2]], port=9042, auth_provider=auth_provider)
    session = cluster.connect()
    flag = False
  except cassandra.cluster.NoHostAvailable as er:
    print(er)
    print('This time cassandra did not answer, program will sleep for 40s and  try again')
    time.sleep(40)

def create_table():

  # Create keyspace
  s = "CREATE KEYSPACE IF NOT EXISTS k_means_results WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':1};"
  executions(session, s)
  
  # Connect to keyspace
  executions(session, f"USE k_means_results;")
  
  # Create table
  q = f"CREATE TABLE IF NOT EXISTS spark1 (prediction int, packaging int, fat_g float, carbohydrates_g float, proteins_g float, nutrition_score int, fp_lat float, fp_lon float, id int, PRIMARY KEY(id));"
  executions(session, q)
    
create_table()

print("KEYSPACE AND TABLE WERE CREATED")