import json
import logging

from google.protobuf.json_format import MessageToJson
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer

from build.gen import rb_announcement_pb2
from build.gen.rb_announcement_pb2 import Corporate
from rb_crawler.constant import SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVER, TOPIC

log = logging.getLogger(__name__)
class RbProducer:
    def __init__(self):
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        protobuf_serializer = ProtobufSerializer(
            rb_announcement_pb2.Corporate, schema_registry_client, {"use.deprecated.format": True}
        )

        producer_conf = {
            "bootstrap.servers": BOOTSTRAP_SERVER,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": protobuf_serializer,
        }

        self.producer = SerializingProducer(producer_conf)

    def produce_to_topic(self, corporate: Corporate):
        self.producer.produce(
            topic=TOPIC, partition=-1, key=str(corporate.id), value=corporate, on_delivery=self.delivery_report
        )

        # It is a naive approach to flush after each produce this can be optimised
        self.producer.poll()
        
        # db = client["infIntDatabase"]
        # collection = db["rb_announcements"]
        # collection.insert_one(json.loads(MessageToJson(corporate)))


    @staticmethod
    def delivery_report(err, msg):
        """
        Reports the failure or success of a message delivery.
        Args:
            err (KafkaError): The error that occurred on None on success.
            msg (Message): The message that was produced or failed.
        Note:
            In the delivery report callback the Message.key() and Message.value()
            will be the binary format as encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s) for key and value to delivery
            report callback we recommend a bound callback or lambda where you pass
            the objects along.
        """
        if err is not None:
            log.error("Delivery failed for User record {}: {}".format(msg.key(), err))
            return
        log.info(
            "User record {} successfully produced to {} [{}] at offset {}".format(
                msg.key(), msg.topic(), msg.partition(), msg.offset()
            )
        )
