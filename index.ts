import { BigQuery, JobLoadMetadata} from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';

const bigquery = new BigQuery();
const storage = new Storage();

const BUCKET_NAME = 'sample-bigquery-study';
const CSV_FILE_NAME = 'MOCK_DATA.csv';

const BQ_DATASET_ID = 'cloud_storage_study_bigquery';
const BQ_TABLE_ID = 'users_with_ts';

async function loadCSVToBigQueryFromGCS() {

    const metadata: JobLoadMetadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        autodetect: true,
    }

    const [job] = await bigquery
    .dataset(BQ_DATASET_ID)
    .table(BQ_TABLE_ID)
    .load(storage.bucket(BUCKET_NAME).file(CSV_FILE_NAME), metadata);

    console.log(`Job ${job.id} completed.`);

    const errors = job.status?.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }  
}

loadCSVToBigQueryFromGCS().catch(console.error);