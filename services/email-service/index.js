import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "email-service" });

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topics: ["order-successful"],
      fromBeginning: false,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, orderId } = JSON.parse(value);

        const emailId = "123456789";
        console.log(`Email consumer: Письмо отправлено пользователю ${userId}`);

        await producer.send({
          topic: "order-successful",
          messages: [{ value: JSON.stringify([userId, emailId]) }],
        });
      },
    });
  } catch (err) {
    console.error(err);
  }
};

run();
