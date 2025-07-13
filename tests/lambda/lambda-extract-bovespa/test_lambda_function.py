import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
import base64

import sys
sys.path.append('src/lambda/lambda-extract-bovespa')
import lambda_function as lf

class TestLambdaExtractBovespa(unittest.TestCase):
    def setUp(self):
        self.api_conf = {
            "host": "api.b3.com.br",
            "route": "portfolio",
            "parameters": {
                "language": "pt",
                "pageNumber": 1,
                "pageSize": 10,
                "index": "IBOV",
                "segment": "ALL"
            }
        }
        self.event = {
            "s3_bucket": "bucket",
            "s3_prefix": "prefix",
            "api": self.api_conf
        }

    def test_decode_encode_api_params(self):
        params = {"foo": "bar"}
        encoded = lf.encode_json_to_api(params)
        decoded = lf.decode_api_params(encoded)
        self.assertEqual(decoded, params)

    def test_build_b3_url_valid(self):
        url = lf.build_b3_url(self.api_conf)
        self.assertTrue(url.startswith("https://api.b3.com.br/portfolio/"))

    def test_build_b3_url_invalid(self):
        conf = self.api_conf.copy()
        conf["host"] = "api.b3.com.br//"
        with self.assertRaises(ValueError):
            lf.build_b3_url(conf)

    @patch("lambda_function.build_b3_url", return_value="http://test")
    @patch("lambda_function.requests.Session.get")
    def test_get_portfolio_day_success(self, mock_get, mock_url):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"foo": "bar"}
        mock_get.return_value = mock_resp
        session = MagicMock()
        session.get = mock_get
        result = lf.get_portfolio_day(self.api_conf, session)
        self.assertEqual(result, {"foo": "bar"})

    def test_portfolio_day_to_df_success(self):
        json_data = {
            "page": {"pageNumber": 1, "pageSize": 1, "totalRecords": 1, "totalPages": 1},
            "header": {
                "date": "01/01/24",
                "text": "t",
                "part": "p",
                "partAcum": "pa",
                "textReductor": "tr",
                "reductor": "r",
                "theoricalQty": "q"
            },
            "results": [
                {
                    "segment": "seg",
                    "cod": "c",
                    "asset": "a",
                    "type": "t",
                    "part": "p",
                    "partAcum": "pa",
                    "theoricalQty": "q"
                }
            ]
        }
        df = lf.portfolio_day_to_df(json_data)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

    def test_portfolio_day_to_df_empty_results(self):
        json_data = {
            "page": {"pageNumber": 1, "pageSize": 1, "totalRecords": 1, "totalPages": 1},
            "header": {
                "date": "01/01/24",
                "text": "t",
                "part": "p",
                "partAcum": "pa",
                "textReductor": "tr",
                "reductor": "r",
                "theoricalQty": "q"
            },
            "results": []
        }
        df = lf.portfolio_day_to_df(json_data)
        self.assertTrue(df.empty)

    def test_portfolio_day_to_df_key_error(self):
        with self.assertRaises(ValueError):
            lf.portfolio_day_to_df({})

    def test_validate_event_success(self):
        valid, msg = lf.validate_event(self.event)
        self.assertTrue(valid)

    def test_validate_event_missing_fields(self):
        event = {}
        valid, msg = lf.validate_event(event)
        self.assertFalse(valid)

    @patch("lambda_function.requests.Session")
    @patch("lambda_function.wr.s3.to_parquet")
    @patch("lambda_function.get_portfolio_day")
    @patch("lambda_function.portfolio_day_to_df")
    def test_lambda_handler_success(self, mock_to_df, mock_get_portfolio, mock_to_parquet, mock_session):
        event = self.event.copy()
        mock_to_df.return_value = pd.DataFrame([{"a": 1, "year": 2024, "month": 1, "day": 1}])
        mock_get_portfolio.return_value = {}
        mock_to_parquet.return_value = None
        mock_session.return_value = MagicMock()
        result = lf.lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 200)

    @patch("lambda_function.requests.Session")
    @patch("lambda_function.get_portfolio_day")
    @patch("lambda_function.portfolio_day_to_df")
    def test_lambda_handler_empty_df(self, mock_to_df, mock_get_portfolio, mock_session):
        event = self.event.copy()
        mock_to_df.return_value = pd.DataFrame()
        mock_get_portfolio.return_value = {}
        mock_session.return_value = MagicMock()
        result = lf.lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 204)

    @patch("lambda_function.requests.Session")
    @patch("lambda_function.get_portfolio_day", side_effect=Exception("fail"))
    def test_lambda_handler_exception(self, mock_get_portfolio, mock_session):
        event = self.event.copy()
        mock_session.return_value = MagicMock()
        result = lf.lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 500)

    def test_lambda_handler_invalid_event(self):
        event = {}
        result = lf.lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 400)

if __name__ == "__main__":
    unittest.main()