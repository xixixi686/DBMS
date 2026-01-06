from pymongo import MongoClient
import time
import psutil
import os

DB_NAME = "experiment.db"

def run_experiment():
    print("-----Program started-----")

        
    client = MongoClient("mongodb://localhost:27017/")
    client.drop_database(DB_NAME)
    db = client[DB_NAME] 
    collection = db[COLLECTION_NAME]
    collection.drop()

    print("Database connected!")



    #Insert Dataset
    ROWS_list = [1000,5000,10000]  #test for scalability


    for ROWS in ROWS_list:
        collection.drop() 
        print(f"\n--- Testing dataset size: {ROWS} ---")
        
        data = []
        
        for i in range(1, ROWS + 1):
            dept = "FINANCE" if i % 3 == 0 else "HR"
            data.append({
                "emp_id" :i, "dept_name": dept, "sensitive_info": f"Secret_{i}"
                })
    
        collection.insert_many(data)
        print("Data inserted.")
    
        ITERATIONS = 50 
    
    
        #RBAC
        print("\nRunning RBAC...")
        process = psutil.Process(os.getpid()) #test for system overhead
        cpu_start = process.cpu_times()  
        
        start = time.perf_counter() #test for latency
    
        
        for _ in range(ITERATIONS):
            rbac_allowed_records = list(collection.find({"dept_name": "FINANCE"}))
            

        rbac_latency = ((time.perf_counter() - start) / ITERATIONS) * 1000
        cpu_end = process.cpu_times()
        rbac_cpu_time = (((cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system))/ITERATIONS)* 1000
        rbac_overhead =  rbac_latency - rbac_cpu_time
        
        print(f"RBAC Average Latency: {rbac_latency:.4f} ms")
        print(f"RBAC System Overhead: {rbac_overhead:.4f} ms")
        print(f"Total records: {len(list(collection.find()))}\nAccessible by policy (RBAC): {len(rbac_allowed_records)}")
    
        #ABAC
        print("\nRunning ABAC...")
        process = psutil.Process(os.getpid())
        cpu_start = process.cpu_times() 
    
        start = time.perf_counter()
        for _ in range(ITERATIONS):
            records = list(collection.find())
            
            abac_allowed_records = [
                r for r in records
                    if
                        r["dept_name"] == "FINANCE"
                        and r["emp_id"] % 2 == 0
                        and len(r["sensitive_info"]) > 6
                ]
    
        abac_latency = ((time.perf_counter() - start) / ITERATIONS) * 1000
        cpu_end = process.cpu_times()
        abac_cpu_time = (((cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system))/ITERATIONS)* 1000
        abac_overhead = abac_latency - abac_cpu_time
        
        print(f"ABAC Average Latency: {abac_latency:.4f} ms")
        print(f"ABAC System Overhead: {abac_overhead:.4f} ms")
        print(f"Total records: {len(records)}\nAccessible by policy (ABAC): {len(abac_allowed_records)}")
    
    client.close()
    print("\n-----Program finished-----")

if __name__ == "__main__":
    run_experiment()
