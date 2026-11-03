// notificacao-consumer.ts
import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'servico-notificacao', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'notificacao-push-group' });

async function run() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'eventos-academicos', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const evento = JSON.parse(message.value?.toString() || '{}');

        if (evento.tipo === 'NOVA_NOTA') {
          console.log(`[Push Notification] Disparando alerta para o app mobile: "${evento.mensagemPush}" (Partição: ${partition})`);
        }
      },
    });
  } catch (error) {
    console.error('[Notificação] Falha no consumer do Kafka:', error);
  }
}

process.on('SIGINT', async () => {
  await consumer.stop();
  process.exit(0);
});

run();