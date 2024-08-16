from unittest.mock import patch

# from src.extract.extract_data import extract_func
# from src.extract.load_data import load
# from src.extract.log_time import get_timestamp_from_logs
#from src.extract.extract_lambda import lambda_handler
# from src.extract.extract_lambda import lambda_handler, extract_func, load, get_timestamp_from_logs
import logging


"""possible tests:
mock extract func to return exception
make of other functions, check invocation?
do I need to put params in to mock somehow? 
need to patch basically all the functions within - multiple patches in one
(this allowed? - yes, @ @ @ @ multiple times)
"""

# with open("s3_bucket_name.txt") as file:
#     os.environ["S3_BUCKET_NAME"] = file.readline()

# @patch("src.extract.extract_lambda.extract_func", side_effect=Exception )
# def test_lambda_handler_catches_exceptions(caplog):
#     with caplog.at_level(logging.ERROR):
#         with pytest.raises(Exception):
#             lambda_handler()
#             assert "Unexpected Exception" in caplog.text


# @patch(src.extract.extract_lambda.extract_func)
# @patch(src.extract.extract_lambda.load)
# @patch(src.extract.extract_lambda.get_timestamp_from_logs)
# def test_lambda_handler_invokes_sub_functions_with_expected_values(
#         caplog, mock_timestamp, mock_load, mock_extract
#         ):
#     mock_timestamp.return_value = "2000:00:00 00:00:00"
#     mock_extract.return_value = "test"
#     with caplog.at_level(logger.INFO)
#     lambda_handler(0,0)

#     mock_extract.assert_called_once()
#     mock_timestamp.assert_called_once()
#     mock_load.assert_called_once()
#     mock_extract.assert_called_with("2000:00:00 00:00:00")
#     mock_load.assert_called_with("test")
#     assert "Extract Lambda ran successfully" in caplog.text
