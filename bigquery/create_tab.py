from google.cloud import bigquery
project = 'graceful-disk-371010'
dataset_id = 'second_dataset'
table_id = 'create_test_table'
client = bigquery.Client(project=project)

sql = """
CREATE VIEW `{}.{}.{}`
OPTIONS(
    expiration_timestamp=TIMESTAMP_ADD(
        CURRENT_TIMESTAMP(), INTERVAL 48 HOUR),
    friendly_name="new_view",
    description="a view that expires in 2 days",
    labels=[("org_unit", "development")]
)
AS SELECT name, state, year, number
    FROM `bigquery-public-data.usa_names.usa_1910_current`
    WHERE state LIKE 'W%'
""".format(
    project, dataset_id, table_id
)

job = client.query(sql)  # API request.
job.result()  # Waits for the query to finish.

print(
    'Created new view graceful-disk-371010.first_dataset.create_test_table".'.format(
        job.destination.project,
        job.destination.dataset_id,
        job.destination.table_id,
    )
)