import asyncio
import pgqrs
import os

# Ensure connection string is set
DSN = os.getenv("PG_DSN", "postgres://postgres:postgres@localhost:5432/postgres")

async def main():
    print(f"Connecting to {DSN}...")
    try:
        store = await pgqrs.connect(DSN)
    except pgqrs.PgqrsConnectionError as e:
        print(f"Failed to connect: {e}")
        return

    admin_api = pgqrs.admin(store)

    print("Installing schema...")
    await admin_api.install()

    queue = "demo_queue"
    try:
        await admin_api.create_queue(queue)
        print(f"Queue '{queue}' created.")
    except pgqrs.QueueAlreadyExistsError:
        print(f"Queue '{queue}' already exists.")

    producer = await store.producer(queue)

    print("Producing 10 messages...")
    for i in range(10):
        msg_id = await pgqrs.enqueue(producer, {"data": f"Hello {i}", "idx": i})
        print(f"Produced message {msg_id}")

    print("Consuming messages using iterator...")
    count = 0
    # Use explicit iterator to access methods like archive()
    iterator = store.consume_iter(queue)
    try:
        async with asyncio.timeout(5.0):
            async for msg in iterator:
                print(f"Received: {msg.id} - {msg.payload}")

                # Simulate processing
                await asyncio.sleep(0.1)

                # Archive the message using the iterator
                await iterator.archive(msg.id)
                print(f"Archived message {msg.id}")

                count += 1
                if count >= 10:
                    break
    except asyncio.TimeoutError:
        print("Timed out waiting for messages.")

    print("Demo complete.")

if __name__ == "__main__":
    asyncio.run(main())
