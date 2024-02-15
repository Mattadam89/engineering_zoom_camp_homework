import dlt

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

data = []
for person in people_2():
    data.append(person)

pipeline = dlt.pipeline(pipeline_name="homework_data_q4",
                        destination = 'duckdb', 
                        dataset_name='homework')

info = pipeline.run(data, 
                    table_name = 'people',
                    write_disposition="merge",
                    primary_key="ID")
print(info)

# SQL query to get sum of ages:

# select SUM(age)
# From people

