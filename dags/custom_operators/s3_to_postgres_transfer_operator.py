from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pandas import read_csv
from io import StringIO

from log import log

@log
class S3ToPostgresTransferOperator(BaseOperator):
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(
        self,
        aws_conn_id,
        s3_key,
        s3_bucket_name,
        postgres_conn_id,
        postgres_table,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_key = s3_key
        self.s3_bucket_name = s3_bucket_name
        self.postgres_conn_id = postgres_conn_id
        self.postgres_table = postgres_table

    def __repr__(self):
        return (
            f'Executing transfer of {self.s3_key} '
            f'from {self.s3_bucket_name} '
            f'to {self.postgres_table}'
        )
    
    def execute(self, context):
        try:
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_data = s3_hook.read_key(self.s3_key, self.s3_bucket_name)
            df = read_csv(StringIO(s3_data))

            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            s_buf = StringIO()
            df.to_csv(s_buf, index=False, header=False)
            s_buf.seek(0)

            try:
                cursor.copy_from(s_buf, self.postgres_table, sep=',')
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error('Records not inserted into the table %s', self.postgres_table)
                self.logger.error(e)
            finally:
                cursor.close()
                conn.close()

        except Exception as err:
            print(err)
            raise err