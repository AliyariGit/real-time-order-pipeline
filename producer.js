const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function run() {
  await producer.connect();
  console.log("🚀 Producer started...");

  setInterval(async () => {
    const order = {
      orderId: Date.now(),
      user: "Reza",
      item: ["Pizza", "Burger", "Sushi"][Math.floor(Math.random() * 3)],
      status: "created",
      timestamp: new Date().toISOString()
    };

    await producer.send({
      topic: 'orders',
      messages: [{ value: JSON.stringify(order) }],
    });

    console.log("✅ Sent:", order);
  }, 3000);
}

run().catch(console.error);
