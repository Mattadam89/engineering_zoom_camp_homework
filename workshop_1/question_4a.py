import dlt

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

data = []
for person in people_1():
    data.append(person)

pipeline = dlt.pipeline(pipeline_name="homework_data_q4",
                        destination = 'duckdb', 
                        dataset_name='homework')

info = pipeline.run(data, 
                    table_name = 'people')
print(info)

