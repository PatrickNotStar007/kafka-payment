import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "analytic-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "analytic-service" });

const run = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: false,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        switch (topic) {
          case "payment-successful":
            {
              const value = message.value.toString();
              const { userId, cart } = JSON.parse(value);

              console.log(`Analytic consumer: Оплата пользователя ${userId}`);
            }
            break;
          case "order-successful":
            {
              const value = message.value.toString();
              const { userId, orderId } = JSON.parse(value);

              console.log(
                `Analytic consumer: заказ ${orderId} создан для пользователя ${userId}`,
              );
            }
            break;
          case "email-successful":
            {
              const value = message.value.toString();
              const { userId, emailId } = JSON.parse(value);

              console.log(
                `Analytic consumer: Письмо ${emailId} отправлено пользователю ${userId}`,
              );
            }
            break;
        }
      },
    });
  } catch (err) {
    console.error(err);
  }
};

run();
