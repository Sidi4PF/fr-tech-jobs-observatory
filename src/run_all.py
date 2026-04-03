import subprocess

print("=== Collecting data ===")
subprocess.run(["python", "collect.py"], check=True)

print("=== Cleaning data ===")
subprocess.run(["python", "clean.py"], check=True)

print("=== Transforming data ===")
subprocess.run(["python", "transform.py"], check=True)

print("=== Pipeline completed successfully ===")
