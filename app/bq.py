from google.cloud import bigquery
from datetime import datetime


def update_table(project, dataset, table, job_id, status, input_bucket_uri, output_bucket_uri, job_template):
    project_id="kishorerjbloom"
    dataset_id="test_sample"
    table_id="trancoder_job_dtls"
    table_id = project+ "." + dataset + "." + table
    client = bigquery.Client()
    query_text = f"""
    UPDATE `{project_id}.{dataset_id}.{table_id}`
    SET status = "FAILED"
    WHERE job_id="Phred Phlyntstone"
    """
    query_job = client.query(query_text)

    # Wait for query job to finish.
    query_job.result()


def insert_table(project, dataset, table, job_id, status, input_bucket_uri, output_bucket_uri, job_template):
    client = bigquery.Client()
    table_id = project+ "." + dataset + "." + table #"kishorerjbloom.test_sample.trancoder_job_dtls"
    # datetime object containing current date and time
    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
     
    rows_to_insert = [
        {u"job_id": job_id,  "status": status, u"start_date":now,u"end_date":now,
        u"input_media_uri": input_bucket_uri, u"output_transcoded_uri": output_bucket_uri,
        u"job_template": job_template}]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

if __name__ == "__main__":
    #insert_table()
    print("main hello")
