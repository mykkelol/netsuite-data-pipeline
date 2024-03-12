from csv import writer
from tempfile import NamedTemporaryFile
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from log import log

@log
class SuiteExport:
    def __init__(self, conn_id, bucket_name, filename, results):
        self.conn_id = conn_id
        self.bucket_name = bucket_name
        self.filename = filename
        self.results = results

    def _get_s3_hook(self):
        return S3Hook(
            aws_conn_id=self.conn_id
        )

    def flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(
                    self.flatten(
                        v, new_key, sep=sep
                    ).items()
                )
            else:
                items.append((new_key, v))
        return dict(items)

    def load_to_s3(self):
        filename = f'{self.filename}.csv'
        results = [self.flatten(d) for d in self.results]
        results_header = (col for col in results[0].keys())
        
        with NamedTemporaryFile(mode='w') as f:
            csv_writer = writer(f)
            csv_writer.writerow(results_header)
            
            for row in results:
                csv_writer.writerow(row.values())
            
            f.flush()

            s3_hook = self._get_s3_hook()
            s3_hook.load_file(
                filename=f.name,
                key=filename,
                bucket_name=self.bucket_name,
                replace=True
            )

        self.logger.info(f'SUCCESS: {filename} loaded to {self.bucket_name}')
        return s3_hook