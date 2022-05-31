const express = require("express");
const kafka = require("kafka-node");
const mongoose = require("mongoose");

const app = express();

app.use(express.json());

const dbsAreRunning = () => {
  mongoose.connect(process.env.MONGO_URL);

  const User = new mongoose.model("user", {
    name: String,
    email: String,
    password: String,
  });

  const client = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });

  const consumer = new kafka.Consumer(
    client,
    [{ topic: process.env.KAFKA_TOPIC }],
    {
      autoCommit: false,
    }
  );

  consumer.on("message", async (message) => {
    const user = await User(JSON.parse(message.value));
    await user.save();
  });

  consumer.on("error", (err) => {
    console.log(err);
  });
};

setTimeout(dbsAreRunning, 30000);

app.listen(process.env.PORT, () =>
  console.log(`App1 is started at port ${process.env.PORT}`)
);
