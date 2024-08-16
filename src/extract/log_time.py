import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import logging


class InvalidInput(Exception):
    pass


def get_timestamp_from_logs(log_group_name="/aws/lambda/extract-de_rossolimo"):
    """
    Gets the time of the most recent cloudwatch log event that was not
    an error for the provided log group name

    Parameters:
    log_group_name: name of log group you require event time from

    returns:
    time in yyyy:mm:dd HH:MM:SS format
    """

    logger = logging.getLogger("timestamp_logger")
    logger.setLevel(logging.INFO)
    # selects the most recent log from the named log stream
    try:
        if not log_group_name:
            raise InvalidInput
        client = boto3.client("logs")
        response_stream = client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy="LastEventTime",
            descending=True,
            limit=10,
        )
        for log_stream_name in response_stream["logStreams"]:

            # filters excluding errors, takes start time of last event
            response = client.filter_log_events(
                logGroupName=log_group_name,
                logStreamNames=[log_stream_name["logStreamName"]],
                filterPattern='-"ERROR"',  # Exclude logs containing 'Error'
                interleaved=True,
            )
            if response["events"]:
                last_event_time = int(response["events"][0]["timestamp"])
                break

        # result is the time we can use, converted to a time that works in SQL
        result = datetime.fromtimestamp(last_event_time / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        print(result)
        logger.info({"Result": "Success"})

    except ClientError as c:
        logger.error({"Result": "Failure", "Error": c.response})

        return {"Result": "Failure", "Error": c.response}
    except InvalidInput as i:
        logger.error(
            {
                "Result": "Failure",
                "Error": "log_group_name parameter must be a valid log group",
            }
        )
        return {"Result": "Failure", "Error": repr(i)}

    except Exception as e:
        logger.error(
            {"Result": "Failure", "Error": f"Exception has occured: {repr(e)}"}
        )
        return {"Result": "Failure", "Error": f"Exception has occured: {repr(e)}"}

    return result
