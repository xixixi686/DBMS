import sqlite3
import time
import os

DB_NAME = "experiment.db"

def run_experiment():
    print("-----Program started-----")

    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    print("Database connected!")

    #Create table
    cursor.execute("""
        CREATE TABLE EMPLOYEE_RECORDS (
            emp_id INTEGER PRIMARY KEY,
            dept_name TEXT,
            sensitive_info TEXT
        )
    """)

    #Insert Dataset
    ROWS = 10000 
    data = []

    for i in range(1, ROWS + 1):
        dept = "FINANCE" if i % 3 == 0 else "HR"
        data.append((i, dept, f"Secret_{i}"))

    cursor.executemany(
        "INSERT INTO EMPLOYEE_RECORDS VALUES (?, ?, ?)",
        data
    )
    conn.commit()
    print("Data insert.")

    ITERATIONS = 50 

    #RBAC
    print("\nRunning RBAC...")
    start = time.perf_counter()

    for _ in range(ITERATIONS):
        cursor.execute(
            "SELECT * FROM EMPLOYEE_RECORDS WHERE dept_name='FINANCE'"
        )
        cursor.fetchall()

    rbac_latency = ((time.perf_counter() - start) / ITERATIONS) * 1000
    print(f"RBAC Average Latency: {rbac_latency:.4f} ms")

    #ABAC
    print("\nRunning ABAC...")
    start = time.perf_counter()

    for _ in range(ITERATIONS):
        cursor.execute("SELECT * FROM EMPLOYEE_RECORDS")
        records = cursor.fetchall()

        for r in records:
            _ = (
                r[1] == "FINANCE"
                and r[0] % 2 == 0
                and len(r[2]) > 6
            )

    abac_latency = ((time.perf_counter() - start) / ITERATIONS) * 1000
    print(f"ABAC Average Latency: {abac_latency:.4f} ms")

    cursor.close()
    conn.close()
    print("\n-----Program finished-----")

if __name__ == "__main__":
    run_experiment()
