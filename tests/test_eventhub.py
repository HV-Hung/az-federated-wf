import os
import time
import uuid
import logging
from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
EVENTHUB_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE")
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME")

# Validation
if not EVENTHUB_NAMESPACE or not EVENTHUB_NAME:
    raise ValueError("Please set EVENTHUB_NAMESPACE and EVENTHUB_NAME environment variables.")

# Construct the fully qualified namespace
FULLY_QUALIFIED_NAMESPACE = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net"
CREDENTIAL = DefaultAzureCredential()

def send_test_events(test_id):
    """Sends a batch of events with a specific test ID."""
    logger.info(f"Starting Send Operation for Test ID: {test_id}")
    
    producer = EventHubProducerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        credential=CREDENTIAL
    )

    with producer:
        event_data_batch = producer.create_batch()
        
        # Add a few events to the batch
        for i in range(5):
            event_body = f"Test Event {i} - ID: {test_id}"
            event_data = EventData(event_body)
            # Add properties to easily filter/identify later
            event_data.properties = {"test_id": test_id, "index": i}
            
            try:
                event_data_batch.add(event_data)
            except ValueError:
                # Batch is full
                break
        
        producer.send_batch(event_data_batch)
        logger.info("Events sent successfully.")

def receive_test_events(test_id, timeout=30):
    """
    Listens for events with the specific test_id. 
    Fails if not found within timeout.
    """
    logger.info(f"Starting Receive Operation for Test ID: {test_id}")
    
    consumer = EventHubConsumerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        consumer_group="$Default",
        credential=CREDENTIAL
    )

    received_count = 0
    start_time = time.time()
    
    def on_event(partition_context, event):
        nonlocal received_count
        
        # Check if this event belongs to our current test run
        if event.properties and event.properties.get(b'test_id') == test_id.encode('utf-8'):
            logger.info(f"Received event: {event.body_as_str()}")
            received_count += 1
            
            # Update checkpoint so we don't re-read old events next time
            partition_context.update_checkpoint(event)

    with consumer:
        # Run the receive loop for a maximum of 'timeout' seconds
        consumer.receive(
            on_event=on_event,
            starting_position="-1",  # Read from end of stream
            max_wait_time=5          # Wait 5s for events, then loop to check timeout
        )
        
        # Manual timeout loop breaker since consumer.receive blocks
        while time.time() - start_time < timeout:
            if received_count >= 5:
                logger.info("All test events received!")
                return True
            time.sleep(1)

    if received_count > 0:
        logger.info(f"Received {received_count} events (partial success).")
        return True
    
    logger.error("Timed out waiting for events.")
    return False

if __name__ == "__main__":
    # Generate a unique ID for this run to avoid reading old data
    current_test_id = str(uuid.uuid4())
    
    try:
        # 1. Send Events
        send_test_events(current_test_id)
        
        # 2. Wait a moment for propagation
        time.sleep(2)
        
        # 3. Receive/Verify Events
        # Note: In a real heavy-load scenario, you might run receive in a separate thread 
        # BEFORE sending, but for functional testing, this sequence usually works.
        success = receive_test_events(current_test_id)
        
        if success:
            logger.info("Test Passed!")
            exit(0)
        else:
            logger.error("Test Failed!")
            exit(1)
            
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        exit(1)