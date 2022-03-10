from airflow.models import BaseOperator
from airflow.models import Variable
import anomalo
from datetime import date, timedelta
from airflow import AirflowException

class AnomaloPassFail(BaseOperator):
    """
    Validate whether checks on table pass or fail
    """
    def __init__(self, tablename, mustpass, *args, **kwargs):
        self.tablename = tablename
        self.mustpass = mustpass
        self.api_client = anomalo.Client(api_token=Variable.get("ANOMALO_API_SECRET_TOKEN"), host=Variable.get("ANOMALO_INSTANCE_HOST"))
        super().__init__(*args, **kwargs)

    def execute(self, context):
        today = date.today()- timedelta(1)
        d1 = today.strftime("%Y-%m-%d")
        table_id = self.api_client.get_table_information(table_name=self.tablename)['id']
        my_job_id = self.api_client.get_check_intervals(
            table_id=table_id, start=d1, end=None
        )[0]["latest_run_checks_job_id"]
        results = self.api_client.get_run_result(job_id=my_job_id)

        for i in results['check_runs']:
            if ( i['run_config']['_metadata']['check_type'] in self.mustpass):
                if (i['results']['success'] == False):
                    raise AirflowException('Anomalo tests have failed')
                    return "Anomalo tests have failed"
        return results