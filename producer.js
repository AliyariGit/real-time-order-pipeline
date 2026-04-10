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

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());

      order.status = "processed";

      console.log("processed:", order);

      fs.appendFileSync(filePath, JSON.stringify(order) + "\n");
    },
  });
}

run();
