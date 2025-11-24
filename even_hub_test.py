import asyncio
import os
import time
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient, EventHubConsumerClient
from azure.identity.aio import DefaultAzureCredential   # <-- async credential

EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "core-feature-hub")
CONSUMER_GROUP = os.environ.get("EVENTHUB_CONSUMER_GROUP", "python-tester-cg")
FQ_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE_FQDN",
                              "demo-eh-namespace-wgctv6.servicebus.windows.net")

async def send_events(credential):
    print(f"\n[Sender] Connecting to {EVENTHUB_NAME}...")
    producer = EventHubProducerClient(
        fully_qualified_namespace=FQ_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        credential=credential
    )

    async with producer:
        batch = await producer.create_batch()
        print("[Sender] Creating events...")
        for i in range(1, 6):
            event_data = EventData(f"Message {i} sent at {time.strftime('%X')}")
            print(f"  - Added: {event_data.body_as_str()}")
            batch.add(event_data)

        print("[Sender] Sending batch...")
        await producer.send_batch(batch)
        print("[Sender] Batch sent successfully.")

async def receive_events(credential):
    print(f"\n[Receiver] Connecting to consumer group '{CONSUMER_GROUP}'...")
    consumer = EventHubConsumerClient(
        fully_qualified_namespace=FQ_NAMESPACE,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        credential=credential
    )

    received_count = 0

    async def on_event(partition_context, event):
        nonlocal received_count
        received_count += 1
        print(f"[Receiver] Received event from partition {partition_context.partition_id}: "
              f"\"{event.body_as_str(encoding='UTF-8')}\"")
        # await partition_context.update_checkpoint(event)  # optional

    async with consumer:
        print("[Receiver] Listening for events (10s)...")
        task = asyncio.create_task(
            consumer.receive(on_event=on_event, starting_position="-1")
        )
        await asyncio.sleep(10)
        print("\n[Receiver] Stopping listener after 10 seconds...")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

async def main():
    # Use async credential as a context manager so it is cleaned up properly
    async with DefaultAzureCredential() as credential:
        await send_events(credential)
        await asyncio.sleep(2)
        await receive_events(credential)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
    except Exception as e:
        print(f"Error: {e}")
