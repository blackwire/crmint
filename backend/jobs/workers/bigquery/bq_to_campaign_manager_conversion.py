# Copyright 2024 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Workers to upload offline conversions to Google Campaign Manager API."""

import json
import string

from typing import Any, List

import google.auth
from google.api_core import page_iterator
from googleapiclient import discovery

from jobs.workers.bigquery import bq_batch_worker, bq_worker

CONVERSION_UPLOAD_PROFILE_ID = 'profile_id'
CONVERSION_UPLOAD_JSON_TEMPLATE = 'template'
SCOPES = ['https://www.googleapis.com/auth/ddmconversions']
API_NAME = 'dfareporting'
API_VERSION = 'v4'
LOG_UPLOAD_RESPONSE_DETAILS = 'log_upload_response_details'

# https://developers.google.com/doubleclick-advertisers/guides/conversions_faq
MAX_ALLOWED_CONVERSIONS_PER_REQUEST = 1000


class BQToCampaignManagerConversion(bq_batch_worker.BQBatchDataWorker):
  """Worker that reads conversions from a BQ table and uploading into Campaign Manager.

  This worker supports uploading conversions, where a GCLID is provided for each
  conversion being uploaded. The conversions with their GCLID's should be
  in a BigQuery table specified by the parameters.
  """

  PARAMS = [
    (bq_worker.BQ_PROJECT_ID_PARAM_NAME,
     'string',
     True,
     '',
     'GCP Project ID where the BQ conversions table lives.'),
    (bq_worker.BQ_DATASET_NAME_PARAM_NAME,
     'string',
     True,
     '',
     'Dataset name where the BQ conversions table lives.'),
    (bq_worker.BQ_TABLE_NAME_PARAM_NAME,
     'string',
     True,
     '',
     'Table name where the BQ conversion data lives.'),
    (CONVERSION_UPLOAD_JSON_TEMPLATE,
     'text',
     True,
     '',
     'JSON template of a conversion upload request.'),
    (CONVERSION_UPLOAD_PROFILE_ID,
     'string',
     True,
     '',
     'Campaign Manager Profile ID of the account the conversions will be uploaded for.'),
    (LOG_UPLOAD_RESPONSE_DETAILS,
     'bool',
     False,
     False,
     'Flag determining if each conversion upload response should be logged'),
  ]

  GLOBAL_SETTINGS = []

  def _validate_params(self) -> None:
    err_messages = []

    err_messages += self._validate_bq_params()
    err_messages += self._validate_cm_client_params()

    if err_messages:
      raise ValueError('The following param validation errors occurred:\n' +
                       '\n'.join(err_messages))

  def _validate_bq_params(self) -> List[str]:
    err_messages = []

    if not self._params.get(bq_worker.BQ_PROJECT_ID_PARAM_NAME, None):
      err_messages.append(
        f'"{bq_worker.BQ_PROJECT_ID_PARAM_NAME}" is required.')

    if not self._params.get(bq_worker.BQ_PROJECT_ID_PARAM_NAME, None):
      err_messages.append(
        f'"{bq_worker.BQ_PROJECT_ID_PARAM_NAME}" is required.')

    if not self._params.get(bq_worker.BQ_DATASET_NAME_PARAM_NAME, None):
      err_messages.append(
        f'"{bq_worker.BQ_DATASET_NAME_PARAM_NAME}" is required.')

    if not self._params.get(bq_worker.BQ_TABLE_NAME_PARAM_NAME, None):
      err_messages.append(
        f'"{bq_worker.BQ_TABLE_NAME_PARAM_NAME}" is required.')

    return err_messages

  def _validate_cm_client_params(self) -> List[str]:
    err_messages = []

    if not self._params.get(CONVERSION_UPLOAD_PROFILE_ID, None):
      err_messages.append(f'"{CONVERSION_UPLOAD_PROFILE_ID}" is required.')

    if not self._params.get(CONVERSION_UPLOAD_JSON_TEMPLATE, None):
      err_messages.append(f'"{CONVERSION_UPLOAD_JSON_TEMPLATE}" is required.')

    return err_messages

  def _execute(self) -> None:
    """Begin the processing and upload of conversions."""
    self.log_info('Validating parameters now.')
    self._validate_params()
    super()._execute()

  def _get_sub_worker_name(self) -> str:
    return BQToCampaignManagerConversionWorker.__name__


class BQToCampaignManagerConversionWorker(bq_batch_worker.TablePageResultsProcessorWorker):
  """A page results worker for uploading a chunk of conversion data."""

  def _process_page_results(self, page_data: page_iterator.Page) -> None:
    cm_service = self._get_cm_service()
    num_rows = page_data.num_items
    template = string.Template(self._params[CONVERSION_UPLOAD_JSON_TEMPLATE])

    conversions = []
    for idx, row in enumerate(page_data):
      conversion = template.substitute(dict(row.items()))
      conversions.append(json.loads(conversion))

      progress = (idx + 1) / num_rows
      if (len(conversions) == MAX_ALLOWED_CONVERSIONS_PER_REQUEST) or (progress == 1):
        self._send_payload(conversions, cm_service)
        self.log_info(f'Completed {progress:.2%} of the Campaign Manager conversion uploads.')
        conversions = []

    self.log_info('Done with Campaign Manager conversion uploads.')

  def _get_cm_service(self) -> discovery.Resource:
    self.log_info('Setting up Campaign Manager service.')

    credentials = google.auth.default(
      scopes=SCOPES
    )

    return discovery.build(API_NAME, API_VERSION, credentials=credentials)

  def _send_payload(self, payload: List[Any], cm_service: discovery.Resource) -> None:
    request_body = {
      'kind': 'dfareporting#conversionsBatchInsertRequest',
      'conversions': payload
    }

    request = cm_service.conversions().batchinsert(
      profileId=self._params[CONVERSION_UPLOAD_PROFILE_ID],
      body=request_body
    )

    response = request.execute()

    upload_status = response['status']
    self.log_info(
      f'[Conversion Upload] Response status code:  {upload_status["code"]}')
    self.log_info(
      f'[Conversion Upload] Response status message:  {upload_status["message"]}')

    if self._params.get(LOG_UPLOAD_RESPONSE_DETAILS, False):
      self._log_upload_response_error_details(response)

  def _log_upload_response_error_details(self, upload_response: Any) -> None:
    error_details = upload_response['status']['errors']
    for detail in error_details:
      self.log_info(f'Error detail:  {detail}')