import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

faker = Faker()
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "user-behavior-events"
CATEGORIES = ["ELECTRONICS", "APPAREL", "HOME", "BEAUTY"]


def generate_event(user_id, session_id, event_type, product_id, price, category):
    return {
        "event_id": faker.uuid4(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product_id,
        "category": category,
        "price": round(price, 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    print("Starting realtime business stream simulation...")
    while True:
        user_id = f"USR_{random.randint(10000, 99999)}"
        session_id = f"SESS_{random.randint(100000, 999999)}"
        product_id = f"PROD_{random.randint(100, 999)}"
        category = random.choice(CATEGORIES)
        price = random.uniform(10.0, 500.0)

        view_event = generate_event(user_id, session_id, "view", product_id, price, category)
        producer.send(TOPIC, view_event)

        if random.random() < 0.40:
            time.sleep(random.uniform(0.1, 0.5))
            cart_event = generate_event(user_id, session_id, "add_to_cart", product_id, price, category)
            producer.send(TOPIC, cart_event)

            time.sleep(random.uniform(0.2, 0.6))
            if random.random() < 0.30:
                purchase_event = generate_event(user_id, session_id, "purchase", product_id, price, category)
                producer.send(TOPIC, purchase_event)
                print(f"Purchase! Session {session_id} bought {product_id} for ksh.{price:.2f}")
            else:
                print(f"Abandonment! Session {session_id} left {product_id} (ksh{price:.2f}) in cart")

        producer.flush()
        time.sleep(0.3)