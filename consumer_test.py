from json import loads
from kafka import KafkaConsumer

# !!!!!!!!!!!!!!!!!!! 잘안됨 !!!!!!!!!!!!!!!!!!!!!!!!!!
#  
if __name__ == "__main__":

    topic_name = "test-topic-1"

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[
            "192.168.122.17:9092"
        ],
        # auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="test",
        value_deserializer=lambda x: loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    print(consumer.topics())

    print("[begin] get consumer list")

    for message in consumer:

        print(
            "Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s"
            % (
                message.topic,
                message.partition,
                message.offset,
                message.key,
                message.value,
            )
        )

    print("[end] get consumer list")