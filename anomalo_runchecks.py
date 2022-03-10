from airflow.models import BaseOperator
from airflow.models import Variable
import anomalo
import time

class AnomaloRunCheck(BaseOperator):
    """
    Run all checks on a table
    """
    def __init__(self, tablename, *args, **kwargs):
        self.tablename = tablename
        self.api_client = anomalo.Client(api_token=Variable.get("ANOMALO_API_SECRET_TOKEN"), host=Variable.get("ANOMALO_INSTANCE_HOST"))
        super().__init__(*args, **kwargs)

    def execute(self, context):
        table_id = self.api_client.get_table_information(table_name=self.tablename)['id']
        run = self.api_client.run_checks(table_id=table_id)
        
        # Wait until all runs complete
        job_id = run['run_checks_job_id']
        while True:
            run_result = self.api_client.get_run_result(job_id=job_id)
            num_pending_checks = sum(
                [1 for c in run_result["check_runs"] if c["results_pending"]]
            )
            if num_pending_checks == 0:
                break
            else:
                time.sleep(10)
        results = self.api_client.get_run_result(job_id=job_id)
        return results