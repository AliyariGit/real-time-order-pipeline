const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function run() {
  await producer.connect();

  setInterval(async () => {
    const order = {
      orderId: Date.now(),
      user: "Reza",
      item: "Pizza",
      status: "created",
      timestamp: new Date().toISOString()
    };

    await producer.send({
      topic: 'orders',
      messages: [{ value: JSON.stringify(order) }],
    });

    console.log("sent:", order);
  }, 3000);
}

run();
