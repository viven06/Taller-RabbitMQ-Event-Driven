const amqp = require("amqplib");

const RABBITMQ_HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const RABBITMQ_PORT = process.env.RABBITMQ_PORT || 5672;
const RABBITMQ_USER = process.env.RABBITMQ_USER || "guest";
const RABBITMQ_PASS = process.env.RABBITMQ_PASS || "guest";
const EXCHANGE_NAME = "events";
const ROUTING_KEY   = "order.created";

const url = `amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connectWithRetry(retries = 10, delay = 3000) {
  for (let i = 1; i <= retries; i++) {
    try {
      const conn = await amqp.connect(url);
      return conn;
    } catch (err) {
      console.log(`[Node Producer] Attempt ${i}/${retries} failed: ${err.message}. Retrying in ${delay}ms...`);
      await sleep(delay);
    }
  }
  throw new Error("[Node Producer] Could not connect to RabbitMQ after max retries.");
}

async function main() {
  console.log("[Node Producer] Connecting to RabbitMQ...");
  const connection = await connectWithRetry();
  const channel    = await connection.createChannel();

  await channel.assertExchange(EXCHANGE_NAME, "topic", { durable: true });
  console.log(`[Node Producer] Connected. Publishing to exchange='${EXCHANGE_NAME}' key='${ROUTING_KEY}'`);

  let counter = 1;
  const items = ["laptop", "phone", "tablet", "monitor", "keyboard"];

  while (true) {
    const payload = {
      producer:  "nodejs",
      event:     "order.created",
      orderId:   `ORD-${String(counter).padStart(4, "0")}`,
      item:      items[Math.floor(Math.random() * items.length)],
      quantity:  Math.floor(Math.random() * 5) + 1,
      timestamp: new Date().toISOString(),
      seq:       counter,
    };
    const body = Buffer.from(JSON.stringify(payload));
    channel.publish(EXCHANGE_NAME, ROUTING_KEY, body, { persistent: true });
    console.log(`[Node Producer] Sent #${counter}: ${JSON.stringify(payload)}`);
    counter++;
    await sleep(2000);
  }
}

main().catch(console.error);
