const express = require("express");
const kafka = require("kafka-node");
const Sequelize = require("sequelize");

const app = express();

app.use(express.json());

const dbsAreRunning = () => {
  const db = new Sequelize(process.env.POSTGRES_URL);
  const User = db.define("user", {
    name: Sequelize.STRING,
    email: Sequelize.STRING,
    password: Sequelize.STRING,
  });

  db.sync({ force: true });

  const client = new kafka.KafkaClient({
    kafka: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  const producer = new kafka.Producer(client);

  producer.on("ready", () => {
    console.log(`Producer is ready`);

    app.post("/", (req, res) => {
      producer.send(
        [
          {
            topic: process.env.KAFKA_TOPIC,
            messages: JSON.stringify(req.body),
          },
        ],
        async (err, data) => {
          if (err) return console.error(err);

          await User.create(req.body);
          res.status(201).json({ message: "User is created" });
        }
      );
    });
  });
};

setTimeout(dbsAreRunning, 30000);

app.listen(process.env.PORT, () =>
  console.log(`App1 is started at port ${process.env.PORT}`)
);
