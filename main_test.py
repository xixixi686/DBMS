import oracledb
import time

# Configuration (Docker Local Environment)
conn_params = {
    "user": "SYSTEM",
    "password": "Password123", 
    "dsn": "localhost:1521/XEPDB1"
}

def run_experiment():
    try:
        # 1. Establish Connection 
        conn = oracledb.connect(**conn_params)
        cursor = conn.cursor()
        print("Successfully connected to Oracle Database!")

        # 2. Initialize Data (10,000 Records) [cite: 138-139, 124]
        print("Cleaning up and generating 10,000 test records...")
        # Drop table if it exists
        cursor.execute("BEGIN EXECUTE IMMEDIATE 'DROP TABLE EMPLOYEE_RECORDS'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        
        # Create table for testing
        cursor.execute("""
            CREATE TABLE EMPLOYEE_RECORDS (
                emp_id NUMBER PRIMARY KEY,
                dept_name VARCHAR2(50),
                sensitive_info VARCHAR2(100)
            )
        """)
        
        # Batch insert synthetic data
        for i in range(1, 10001):
            dept = 'FINANCE' if i % 3 == 0 else 'HR'
            cursor.execute("INSERT INTO EMPLOYEE_RECORDS VALUES (:1, :2, :3)", [i, dept, f'Secret_{i}'])
        
        conn.commit()
        print("Data initialization complete.")

        # 3. Performance Testing 
        results = {}
        scenarios = {
            "RBAC (Static)": "SELECT * FROM EMPLOYEE_RECORDS WHERE dept_name = 'FINANCE'",
            "ABAC (Dynamic)": "SELECT * FROM EMPLOYEE_RECORDS WHERE dept_name = 'FINANCE' AND TO_CHAR(SYSDATE, 'HH24') BETWEEN '09' AND '18'"
        }

        print("\nStarting performance benchmarking...")
        for name, sql in scenarios.items():
            start_time = time.perf_counter()
            for _ in range(1000):
                cursor.execute(sql)
                cursor.fetchall()
            
            # Calculate average latency in milliseconds (ms)
            avg_latency = ((time.perf_counter() - start_time) / 1000) * 1000 
            results[name] = avg_latency
            print(f"{name} Average Latency: {avg_latency:.4f} ms")

        cursor.close()
        conn.close()
        return results

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_experiment()