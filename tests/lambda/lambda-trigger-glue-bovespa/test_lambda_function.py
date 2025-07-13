import unittest
from unittest.mock import patch, MagicMock
import json

import sys
sys.path.append('src/lambda/lambda-trigger-glue-bovespa')
import lambda_function as lf

class TestLambdaTriggerGlueBovespa(unittest.TestCase):
    def setUp(self):
        self.event = {
            "job_name": "glue-refined-zone-bovespa",
            "job_parameters": {"--JOB_NAME": "glue-refined-zone-bovespa"}
        }

    def test_validate_event_success(self):
        valid, msg = lf.validate_event(self.event)
        self.assertTrue(valid)

    def test_validate_event_missing(self):
        event = {}
        valid, msg = lf.validate_event(event)
        self.assertFalse(valid)
        event = {"job_name": "a"}
        valid, msg = lf.validate_event(event)
        self.assertFalse(valid)
        event = {"job_name": "a", "job_parameters": ""}
        valid, msg = lf.validate_event(event)
        self.assertFalse(valid)

    @patch("lambda_function.boto3.client")
    def test_start_glue_job_success(self, mock_boto):
        mock_client = MagicMock()
        mock_client.start_job_run.return_value = {"JobRunId": "123"}
        mock_boto.return_value = mock_client
        resp = lf.start_glue_job("job", {"--JOB_NAME": "job"})
        self.assertEqual(resp["statusCode"], 200)
        self.assertIn("JobRunId", json.loads(resp["body"]))

    @patch("lambda_function.boto3.client", side_effect=Exception("fail"))
    def test_start_glue_job_fail(self, mock_boto):
        resp = lf.start_glue_job("job", {"--JOB_NAME": "job"})
        self.assertEqual(resp["statusCode"], 500)

    @patch("lambda_function.start_glue_job", return_value={"statusCode": 200, "body": "{}"})
    def test_lambda_handler_success(self, mock_start):
        event = self.event.copy()
        resp = lf.lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 200)

    def test_lambda_handler_invalid(self):
        event = {}
        resp = lf.lambda_handler(event, None)
        self.assertEqual(resp["statusCode"], 400)

if __name__ == "__main__":
    unittest.main()