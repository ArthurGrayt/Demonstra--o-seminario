// historico-consumer.ts
import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'servico-historico', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'historico-group' });

async function run() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'eventos-academicos', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const evento = JSON.parse(message.value?.toString() || '{}');
        console.log(`[Histórico] Atualizando banco de dados e recalculando média do aluno ${evento.matricula}... (Partição: ${partition})`);
      },
    });
  } catch (error) {
    console.error('[Histórico] Falha no consumer do Kafka:', error);
  }
}

process.on('SIGINT', async () => {
  await consumer.stop();
  process.exit(0);
});

run();