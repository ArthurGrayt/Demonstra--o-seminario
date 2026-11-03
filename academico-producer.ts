// academico-producer.ts
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'core-academico',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function run() {
  try {
    await producer.connect();
    const matriculaAluno = 'RA-202610';

    await producer.send({
      topic: 'eventos-academicos',
      messages: [
        {
          key: matriculaAluno,
          value: JSON.stringify({
            matricula: matriculaAluno,
            disciplina: 'Sistemas Distribuídos',
            tipo: 'NOVA_NOTA',
            valor: 9.5,
            mensagemPush: 'Sua nota de Sistemas Distribuídos foi lançada!',
          }),
        },
      ],
    });

    console.log('[SUAP-Core] Evento de nota publicado no Kafka.');
  } catch (error) {
    console.error('[SUAP-Core] Falha ao publicar evento no Kafka:', error);
  } finally {
    await producer.disconnect().catch(() => undefined);
  }
}

run();