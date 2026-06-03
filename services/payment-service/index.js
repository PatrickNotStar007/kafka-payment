import express from "express";
import { Kafka } from "kafkajs";

const app = express();

app.use(express.json());

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();

const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("Продюсер подключён");
  } catch (err) {
    console.log("Ошибка подключения к Kafka", err);
  }
};

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;
  const userId = "123";

  await producer.send({
    topic: "payment-successful",
    messages: [{ value: JSON.stringify({ userId, cart }) }],
  });

  return res.status(200).send("Оплата прошла успешно!");
});

app.use((err, req, res, next) => {
  return res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => {
  connectToKafka();
  console.log("Сервис payment запущен на порту 8000");
});
