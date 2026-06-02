import express from "express";

const app = express();

app.use(express.json());

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;
  const userId = "123";

  return res.status(200).send("Оплата прошла успешно!");
});

app.use((err, req, res, next) => {
  return res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => console.log("Сервис payment запущен на порту 8000"));
