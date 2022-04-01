from airflow.models import BaseOperator
from airflow.models import Variable
from airflow import AirflowException
import anomalo
import time

class AnomaloRunCheck(BaseOperator):
    """
    Run all checks on a table
    """
    def __init__(self, tablename, timeout, *args, **kwargs):
        self.tablename = tablename
        self.timeout = int(15)
        if (self.timeout == 0 or self.timeout is None or self.timeout == ""):
            self.timeout = int(15)
        else:
            self.timeout = int(timeout)
        self.api_client = anomalo.Client(api_token=Variable.get("ANOMALO_API_SECRET_TOKEN"), host=Variable.get("ANOMALO_INSTANCE_HOST"))
        super().__init__(*args, **kwargs)

    def execute(self, context):
        table_id = self.api_client.get_table_information(table_name=self.tablename)['id']
        run = self.api_client.run_checks(table_id=table_id)
        timeout_max = self.timeout * 3
        timeout_counter = 0
        job_id = run['run_checks_job_id']

        # Checks runs every 30s until all runs complete or timeout limit is hit 
        while (timeout_counter < timeout_max):
            run_result = self.api_client.get_run_result(job_id=job_id)
            num_pending_checks = sum(
                [1 for c in run_result["check_runs"] if c["results_pending"]]
            )
            if num_pending_checks == 0:
                break
            else:
                time.sleep(20)
                timeout_counter += 1
        if (num_pending_checks == 0):
            print("All jobs completed")
        elif (num_pending_checks != 0):
            raise AirflowException('Anomalo tests have timed out')
        results = self.api_client.get_run_result(job_id=job_id)
        return results