"""Tests for bq_to_ads_offline_click_conversion.py"""

from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

from jobs.workers.bigquery import bq_to_campaign_manager_conversion


class BQToCampaignManagerConversionTests(parameterized.TestCase):
  def setUp(self):
    super().setUp()

  @mock.patch('jobs.workers.bigquery.bq_batch_worker.BQBatchDataWorker._execute')
  @mock.patch('jobs.workers.worker.Worker.log_info')
  def test_fails_if_required_params_not_provided(
    self, mocked_logger, mocked_execute
  ):
    parameters = {}
    worker = bq_to_campaign_manager_conversion.BQToCampaignManagerConversion(
      parameters, 0, 0
    )

    with self.assertRaises(ValueError) as err_context:
      worker._execute()

    self.assertIn('"template" is required.', str(err_context.exception))
    self.assertIn('"profile_id" is required.', str(err_context.exception))
    self.assertIn('"bq_project_id" is required.', str(err_context.exception))
    self.assertIn('"bq_dataset_id" is required.', str(err_context.exception))
    self.assertIn('"bq_table_id" is required.', str(err_context.exception))

  @mock.patch('jobs.workers.bigquery.bq_batch_worker.BQBatchDataWorker._execute')
  @mock.patch('jobs.workers.worker.Worker.log_info')
  def test_calls_parent_bq_batch_worker_to_start_execution(
    self, mocked_logger, mocked_execute
  ):
    """Validates a successful call when all required parameters are provided."""
    parameters = {
      'bq_project_id': '123',
      'bq_dataset_id': 'a_dataset',
      'bq_table_id': 'a_table',
      'template': 'a_template_string',
      'profile_id':  '123456'
    }

    worker = bq_to_campaign_manager_conversion.BQToCampaignManagerConversion(
      parameters, 'pipeline_id', 'job_id'
    )
    worker._execute()

    mocked_execute.assert_called()


if __name__ == '__main__':
  absltest.main()
