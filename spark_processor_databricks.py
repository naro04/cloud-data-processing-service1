
class SparkProcessorDatabricks:


    def __init__(self, dataset_path):
        self.dataset_path = dataset_path

    def generate_complete_code(self):


        code = f"""


from pyspark.sql.functions import col, count, mean, min, max, stddev, when, avg , sum
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from datetime import datetime
import time
import pandas as pd

print("="*80)
print("CLOUD DATA PROCESSING ANALYSIS")
print("="*80)

# Load dataset
print("\\nLoading dataset...")
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("{self.dataset_path}")
total_rows = df.count()
total_cols = len(df.columns)
print(f"Dataset loaded: {{total_rows:,}} rows, {{total_cols}} columns")

# Storage for results
all_task_times = {{}}
all_results = {{}}

# ============================================================
# TASK 1: DESCRIPTIVE STATISTICS
# ============================================================

print("\\n" + "="*80)
print("TASK 1: DESCRIPTIVE STATISTICS")
print("="*80)

start_time = time.time()

print(f"Total Rows: {{total_rows:,}}")
print(f"Total Columns: {{total_cols}}")

print("\\nData Types:")
for col_name, col_type in df.dtypes:
    print(f"  {{col_name}}: {{col_type}}")

numeric_cols = [name for name, dtype in df.dtypes if dtype in ['int','bigint','double','float']]
print(f"\\nNumeric Statistics:")
df.select(numeric_cols).describe().show()

print("\\nMissing Values:")
has_missing = False
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    if null_count > 0:
        pct = (null_count/total_rows)*100
        print(f"  {{c}}: {{null_count:,}} ({{pct:.2f}}%)")
        has_missing = True
if not has_missing:
    print("  No missing values found")

stats_time = time.time() - start_time
all_task_times['Statistics'] = stats_time
print(f"\\nExecution time: {{stats_time:.2f}} seconds")

# ============================================================
# TASK 2: LINEAR REGRESSION
# ============================================================

print("\\n" + "="*80)
print("TASK 2: LINEAR REGRESSION")
print("="*80)

start_time = time.time()

reg_data = df.select(['distance','delay']).na.drop().filter((col('delay')>=-50)&(col('delay')<=500))
print(f"Training data: {{reg_data.count():,}} rows")

assembler = VectorAssembler(inputCols=['distance'], outputCol='features')
reg_prepared = assembler.transform(reg_data)
train_reg, test_reg = reg_prepared.randomSplit([0.8,0.2], seed=42)

lr = LinearRegression(featuresCol='features', labelCol='delay', maxIter=10)
lr_model = lr.fit(train_reg)
predictions = lr_model.transform(test_reg)
evaluator = RegressionEvaluator(labelCol='delay', predictionCol='prediction')

rmse = evaluator.evaluate(predictions, {{evaluator.metricName:'rmse'}})
r2 = evaluator.evaluate(predictions, {{evaluator.metricName:'r2'}})

print(f"RMSE: {{rmse:.4f}}")
print(f"R2: {{r2:.4f}}")

all_results['regression'] = {{'rmse':rmse, 'r2':r2}}
regression_time = time.time() - start_time
all_task_times['Regression'] = regression_time
print(f"Execution time: {{regression_time:.2f}} seconds")

# ============================================================
# TASK 3: KMEANS CLUSTERING
# ============================================================

print("\\n" + "="*80)
print("TASK 3: KMEANS CLUSTERING")
print("="*80)

start_time = time.time()

cluster_data = df.select(['distance','delay']).na.drop().filter((col('delay')>=-50)&(col('delay')<=500))
assembler = VectorAssembler(inputCols=['distance','delay'], outputCol='features')
cluster_prepared = assembler.transform(cluster_data)

kmeans = KMeans(k=5, seed=42, maxIter=20)
kmeans_model = kmeans.fit(cluster_prepared)
cluster_predictions = kmeans_model.transform(cluster_prepared)

print("Cluster Distribution:")
cluster_predictions.groupBy('prediction').count().orderBy('prediction').show()

clustering_time = time.time() - start_time
all_task_times['Clustering'] = clustering_time
print(f"Execution time: {{clustering_time:.2f}} seconds")

# ============================================================
# TASK 4: TIME SERIES ANALYSIS
# ============================================================

print("\\n" + "="*80)
print("TASK 4: TIME SERIES ANALYSIS")
print("="*80)

start_time = time.time()

ts_data = df.withColumn('date_str',col('date').cast('string'))
ts_data = ts_data.withColumn('year',col('date_str').substr(1,4).cast('int'))
ts_data = ts_data.withColumn('month',col('date_str').substr(5,2).cast('int'))

print("Monthly Analysis:")
monthly = ts_data.groupBy('month').agg(avg('delay').alias('avg_delay'),count('delay').alias('count')).orderBy('month')
monthly.show(12)

timeseries_time = time.time() - start_time
all_task_times['Time Series'] = timeseries_time
print(f"Execution time: {{timeseries_time:.2f}} seconds")

# ============================================================
# TASK 5: CLASSIFICATION
# ============================================================

print("\\n" + "="*80)
print("TASK 5: CLASSIFICATION")
print("="*80)

start_time = time.time()

class_data = df.select(['distance','delay']).na.drop().withColumn('is_delayed',when(col('delay')>15,1).otherwise(0))
assembler = VectorAssembler(inputCols=['distance'], outputCol='features')
class_prepared = assembler.transform(class_data)
train_class, test_class = class_prepared.randomSplit([0.8,0.2], seed=42)

lr_classifier = LogisticRegression(featuresCol='features', labelCol='is_delayed', maxIter=10)
class_model = lr_classifier.fit(train_class)
class_predictions = class_model.transform(test_class)
evaluator = MulticlassClassificationEvaluator(labelCol='is_delayed', predictionCol='prediction')

accuracy = evaluator.evaluate(class_predictions, {{evaluator.metricName:'accuracy'}})
f1 = evaluator.evaluate(class_predictions, {{evaluator.metricName:'f1'}})

print(f"Accuracy: {{accuracy:.4f}}")
print(f"F1 Score: {{f1:.4f}}")

all_results['classification'] = {{'accuracy':accuracy, 'f1':f1}}
classification_time = time.time() - start_time
all_task_times['Classification'] = classification_time
print(f"Execution time: {{classification_time:.2f}} seconds")

# ============================================================
# PERFORMANCE ANALYSIS
# ============================================================

print("\\n" + "="*80)
print("PERFORMANCE ANALYSIS")
print("="*80)

baseline_time = 0.0
for v in all_task_times.values():
    baseline_time += float(v)
    
print(f"\\nBaseline (1 Worker): {{baseline_time:.2f}} seconds")

serial_fraction = 0.10
parallel_fraction = 0.90

def calculate_speedup(n, s, p):
    return 1 / (s + (p/n))

def calculate_efficiency(speedup, n):
    return (speedup/n)*100

worker_configs = [1,2,4,8]
results = []

for workers in worker_configs:
    if workers == 1:
        speedup = 1.0
        exec_time = baseline_time
        efficiency = 100.0
        mtype = "Actual"
    else:
        speedup = calculate_speedup(workers, serial_fraction, parallel_fraction)
        exec_time = baseline_time / speedup
        efficiency = calculate_efficiency(speedup, workers)
        mtype = "Projected"

    results.append({{
        'Workers': workers,
        'Time': round(exec_time,2),
        'Speedup': round(speedup,2),
        'Efficiency': round(efficiency,1),
        'Type': mtype
    }})

df_results = pd.DataFrame(results)

print("\\n" + "="*80)
print("PERFORMANCE METRICS")
print("="*80)
print(df_results.to_string(index=False))
print("="*80)

# ============================================================
# VISUALIZATION
# ============================================================

print("\\nGenerating charts...")

try:
    import matplotlib.pyplot as plt

    fig, ((ax1,ax2),(ax3,ax4)) = plt.subplots(2,2, figsize=(16,12))

    ax1.bar(df_results['Workers'], df_results['Time'], color='skyblue', edgecolor='navy')
    ax1.set_xlabel('Workers')
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('Execution Time vs Workers')
    ax1.grid(True, alpha=0.3, axis='y')

    ax2.plot(df_results['Workers'], df_results['Speedup'], marker='o', linewidth=3, markersize=10, color='green', label='Speedup')
    ax2.plot(df_results['Workers'], df_results['Workers'], linestyle='--', color='red', alpha=0.5, linewidth=2, label='Ideal')
    ax2.set_xlabel('Workers')
    ax2.set_ylabel('Speedup')
    ax2.set_title('Speedup vs Workers')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    ax3.plot(df_results['Workers'], df_results['Efficiency'], marker='s', linewidth=3, markersize=10, color='orange')
    ax3.axhline(y=100, linestyle='--', color='gray', alpha=0.5, linewidth=2)
    ax3.set_xlabel('Workers')
    ax3.set_ylabel('Efficiency (%)')
    ax3.set_title('Efficiency vs Workers')
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(0,110)

    tasks = list(all_task_times.keys())
    times = list(all_task_times.values())
    colors = ['#FF6B6B','#4ECDC4','#45B7D1','#FFA07A','#98D8C8']
    ax4.pie(times, labels=tasks, autopct='%1.1f%%', startangle=90, colors=colors)
    ax4.set_title('Task Time Distribution')

    plt.tight_layout()
    plt.savefig('/dbfs/FileStore/performance_analysis.png', dpi=300, bbox_inches='tight')
    print("Charts saved to: /dbfs/FileStore/performance_analysis.png")
    display(fig)
except Exception as e:
    print(f"Could not generate charts: {{str(e)}}")

# ============================================================
# SAVE RESULTS TO FILES
# ============================================================

print("\\nSaving results...")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Complete text report
report = f\"\"\"
================================================================================
CLOUD DATA PROCESSING - COMPLETE RESULTS
Generated: {{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
================================================================================

DATASET INFORMATION
Path: {self.dataset_path}
Total Rows: {{total_rows:,}}
Total Columns: {{total_cols}}

TASK EXECUTION TIMES
{{chr(10).join([f"{{task}}: {{time:.2f}} seconds" for task,time in all_task_times.items()])}}
Total: {{baseline_time:.2f}} seconds

PERFORMANCE METRICS
Workers | Time (s) | Speedup | Efficiency (%) | Type
{{chr(10).join([f"   {{r['Workers']}}    |  {{r['Time']:6.2f}}  |  {{r['Speedup']:5.2f}}  |     {{r['Efficiency']:5.1f}}     | {{r['Type']}}" for r in results])}}

MACHINE LEARNING RESULTS
Linear Regression: RMSE={{all_results['regression']['rmse']:.4f}}, R2={{all_results['regression']['r2']:.4f}}
Classification: Accuracy={{all_results['classification']['accuracy']:.4f}}, F1={{all_results['classification']['f1']:.4f}}

SCALABILITY ANALYSIS
Maximum theoretical speedup: {{1/serial_fraction:.1f}}x
Efficiency decreases from 100% to {{results[3]['Efficiency']}}% with 8 workers
Optimal configuration: 4 workers ({{results[2]['Speedup']}}x speedup, {{results[2]['Efficiency']}}% efficiency)

================================================================================
\"\"\"

try:
    # Save text report
    report_file = f"results_{{timestamp}}.txt"
    with open(f"/dbfs/FileStore/{{report_file}}", 'w') as f:
        f.write(report)

    # Save performance CSV
    csv_file = f"performance_{{timestamp}}.csv"
    df_results.to_csv(f"/dbfs/FileStore/{{csv_file}}", index=False)

    # Save task times CSV
    task_file = f"task_times_{{timestamp}}.csv"
    pd.DataFrame(list(all_task_times.items()), columns=['Task','Time']).to_csv(f"/dbfs/FileStore/{{task_file}}", index=False)

    print(f"\\nResults saved:")
    print(f"  - {{report_file}}")
    print(f"  - {{csv_file}}")
    print(f"  - {{task_file}}")
    print(f"  - performance_analysis.png")
    print(f"\\nDownload from: Data > Browse DBFS > FileStore")

except Exception as e:
    print(f"Error saving results: {{str(e)}}")

# ============================================================
# SUMMARY
# ============================================================

print("\\n" + "="*80)
print("PROJECT COMPLETE")
print("="*80)
print(f"Total execution time: {{baseline_time:.2f}} seconds")
print(f"All tasks completed successfully")
print(f"Results saved to FileStore with timestamp: {{timestamp}}")
print("="*80)
"""

        return code


if __name__ == "__main__":
    from config import DATASET_PATH

    processor = SparkProcessorDatabricks(DATASET_PATH)

    
    print("COMPLETE CODE GENERATOR")
    


    