import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topics: ["payment-successful"],
      fromBeginning: false,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, cart } = JSON.parse(value);

        const orderId = "123456789";
        console.log(`Order consumer: Заказ создал для пользователя ${userId}`);

        await producer.send({
          topic: "order-successful",
          messages: [{ value: JSON.stringify([userId, orderId]) }],
        });
      },
    });
  } catch (err) {
    console.error(err);
  }
};

run();
