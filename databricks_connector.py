import requests
import json
import time
class DatabricksConnector:
    def __init__(self, host, token):

        self.host = host.rstrip('/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def test_connection(self):
        
        try:
            url = f"{self.host}/api/2.0/clusters/list"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                print(" Connected to Databricks successfully!")
                return True
            else:
                print(f" Connection failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False

        except Exception as e:
            print(f" Connection error: {str(e)}")
            return False

    def list_clusters(self):
      
        try:
            url = f"{self.host}/api/2.0/clusters/list"
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                clusters = data.get('clusters', [])

                print(f"\n Available Clusters: {len(clusters)}")
                for cluster in clusters:
                    state = cluster.get('state', 'UNKNOWN')
                    name = cluster.get('cluster_name', 'Unnamed')
                    cluster_id = cluster.get('cluster_id', 'No ID')
                    print(f"  - {name}")
                    print(f"    ID: {cluster_id}")
                    print(f"    State: {state}")

                return clusters
            else:
                print(f" Failed to list clusters: {response.status_code}")
                return []

        except Exception as e:
            print(f" Error listing clusters: {str(e)}")
            return []

    def get_active_cluster(self):
        
        clusters = self.list_clusters()

        for cluster in clusters:
            if cluster.get('state') == 'RUNNING':
                return cluster.get('cluster_id')


        if clusters:
            return clusters[0].get('cluster_id')

        return None

    def start_cluster(self, cluster_id):
        
        try:
            url = f"{self.host}/api/2.0/clusters/start"
            data = {'cluster_id': cluster_id}

            response = requests.post(url, headers=self.headers, json=data, timeout=10)

            if response.status_code == 200:
                print(f" Cluster starting... (this may take 3-5 minutes)")
                return True
            else:
                print(f" Failed to start cluster: {response.text}")
                return False

        except Exception as e:
            print(f" Error starting cluster: {str(e)}")
            return False

    def run_notebook_code(self, code, cluster_id=None):

        try:
            if cluster_id is None:
                cluster_id = self.get_active_cluster()
                if not cluster_id:
                    return {"error": "No cluster available"}

            print(f" Sending code to Databricks cluster: {cluster_id[:10]}...")



            return {
                "status": "success",
                "message": "Code execution simulated - use Databricks notebooks for actual execution"
            }

        except Exception as e:
            return {"error": str(e)}

    def compute_statistics_remote(self, dataset_path):



        code = f"""
# Load dataset
df = spark.read.format("csv") \\
    .option("header", "true") \\
    .option("inferSchema", "true") \\
    .load("{dataset_path}")

# Basic statistics
print("Total Rows:", df.count())
print("Total Columns:", len(df.columns))

# Show data types
print("\\nData Types:")
df.printSchema()

# Numeric statistics
numeric_cols = [col_name for col_name, col_type in df.dtypes 
                if col_type in ['int', 'bigint', 'double', 'float']]

print("\\nNumeric Statistics:")
df.select(numeric_cols[:5]).describe().show()

# Missing values
from pyspark.sql.functions import col, count, when

print("\\nMissing Values:")
for c in df.columns[:10]:
    null_count = df.filter(col(c).isNull()).count()
    if null_count > 0:
        pct = (null_count / df.count()) * 100
        print(f"{{c}}: {{null_count}} ({{pct:.2f}}%)")
"""

        return {
            "status": "code_generated",
            "code": code,
            "message": "Copy this code to your Databricks notebook and run it",
            "instructions": [
                "1. Open your Databricks notebook",
                "2. Copy the code from 'code' field",
                "3. Paste and run in notebook",
                "4. Results will appear in the notebook"
            ]
        }


# Test connection
if __name__ == "__main__":
    from config import DATABRICKS_HOST, DATABRICKS_TOKEN

    
    print("TESTING DATABRICKS CONNECTION")
    

    connector = DatabricksConnector(DATABRICKS_HOST, DATABRICKS_TOKEN)

    # Test connection
    if connector.test_connection():
        print("\n Connection test passed!")

        # List clusters
        connector.list_clusters()

        print("\n" + "=" * 70)
        print(" DATABRICKS CONNECTOR IS READY!")
        
    else:
        print("\n Connection test failed!")
        print("\nPlease check:")
