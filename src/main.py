import time
import argparse
import logging
import sys

from nidaq_module import init_task, read_task_data

from awsiot.greengrasscoreipc.clientv2 import GreengrassCoreIPCClientV2
from awsiot.greengrasscoreipc.model import JsonMessage, PublishMessage


logger = logging.getLogger(__name__)


def configure_logging( log_level: str ):
    numeric_level = getattr( logging ,log_level.upper() ,None )

    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    logging.basicConfig(stream=sys.stdout, level=numeric_level)
    logger.setLevel(numeric_level)


def publish_message ( ipc_client: GreengrassCoreIPCClientV2 ,topic: str ,message: dict[str ,any] ):
    publish_message = PublishMessage( json_message=JsonMessage( message=message ) )

    return ipc_client.publish_to_topic_async( topic=topic ,publish_message=publish_message )



def main () -> int:
    parser = argparse.ArgumentParser(description="DAQ and IPC publisher component")

    parser.add_argument(
        "--rawdata_topic", 
        type=str, 
        required=True, 
        help="IPC topic to publish messages"
    )
    
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Configure logging
    configure_logging(args.log_level)

    rawdata_topic = args.rawdata_topic


    try:
        ipc_client = GreengrassCoreIPCClientV2()
    except Exception as e:
        logger.error("Failed to create Greengrass IPC client: %s", e)
        sys.exit(1)

    logger.info("Starting DAQ and publishing to topic: %s", rawdata_topic)


    ini_path = "./API/NiDAQ.ini"
    task, samples_per_read, sample_rate, channel_names = init_task( ini_path )
    logger.info("DAQ task initialized with sample rate: %s, samples per read: %s", sample_rate, samples_per_read)

    ipc_client = GreengrassCoreIPCClientV2()

    while True:
        data = read_task_data( task ,samples_per_read )
        logger.debug("Read data")

        message = {
            "timestamp": time.time(),
            "sample_rate": sample_rate,
            "data_len": len( data ),
            "data_header": ",".join( channel_names ),
            "data": data
        }

        publish_message(ipc_client, rawdata_topic, message)

        logger.debug("Published message with timestamp: %s", message["timestamp"])

    return 0


if __name__ == "__main__":
    main()
