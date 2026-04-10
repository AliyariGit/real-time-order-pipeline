const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');

const kafka = new Kafka({
  clientId: 'order-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'order-group' });

const filePath = path.join(__dirname, '../storage/orders.json');

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: true });

  console.log("📡 Consumer started...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());

      // simulate processing
      order.status = "processed";
      order.processedAt = new Date().toISOString();

      console.log("⚙️ Processed:", order);

      fs.appendFileSync(filePath, JSON.stringify(order) + "\n");
    },
  });
}

run().catch(console.error);
