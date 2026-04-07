const amqp = require("amqplib");

const RABBITMQ_HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const RABBITMQ_PORT = process.env.RABBITMQ_PORT || 5672;
const RABBITMQ_USER = process.env.RABBITMQ_USER || "guest";
const RABBITMQ_PASS = process.env.RABBITMQ_PASS || "guest";
const EXCHANGE_NAME = "events";
const QUEUE_NAME    = "queue.nodejs";
const BINDING_KEY   = "order.#";

const url = `amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connectWithRetry(retries = 10, delay = 3000) {
  for (let i = 1; i <= retries; i++) {
    try {
      return await amqp.connect(url);
    } catch (err) {
      console.log(`[Node Consumer] Attempt ${i}/${retries} failed: ${err.message}. Retrying...`);
      await sleep(delay);
    }
  }
  throw new Error("[Node Consumer] Max retries reached.");
}

async function main() {
  console.log("[Node Consumer] Connecting to RabbitMQ...");
  const connection = await connectWithRetry();
  const channel    = await connection.createChannel();

  await channel.assertExchange(EXCHANGE_NAME, "topic", { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, BINDING_KEY);
  await channel.prefetch(1);

  console.log(`[Node Consumer] Waiting for messages on queue='${QUEUE_NAME}' binding='${BINDING_KEY}'...`);

  channel.consume(QUEUE_NAME, (msg) => {
    if (!msg) return;
    try {
      const data = JSON.parse(msg.content.toString());
      console.log("[Node Consumer] Received:", JSON.stringify(data, null, 2));
    } catch {
      console.log("[Node Consumer] Received (raw):", msg.content.toString());
    }
    channel.ack(msg);
  });
}

main().catch(console.error);
