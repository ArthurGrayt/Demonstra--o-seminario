// Importa a biblioteca do cliente Kafka para Node.js
import { Kafka } from 'kafkajs';

// Configura a conexão com o broker local do Kafka
const kafka = new Kafka({ clientId: 'servico-notificacao', brokers: ['localhost:9092'] });

// Cria a instância do consumidor associado ao grupo 'notificacao-push-group'
const consumer = kafka.consumer({ groupId: 'notificacao-push-group' });

// Função principal assíncrona que gerencia o ciclo de vida do consumidor
async function run() {
  try {
    // Conecta o consumidor ao cluster do Kafka
    await consumer.connect();
    // Exibe mensagem informativa de conexão bem-sucedida
    console.log('✅ [Notificação] Conectado ao Kafka com sucesso!');

    // Inscreve o consumidor no tópico 'eventos-academicos' lendo desde o início
    await consumer.subscribe({ topic: 'eventos-academicos', fromBeginning: true });
    // Exibe status de prontidão para receber mensagens
    console.log('🎧 [Notificação] Aguardando mensagens no tópico "eventos-academicos"...\n');

    // Inicia o processamento contínuo de cada mensagem recebida
    await consumer.run({
      // Callback executado para cada mensagem consumida
      eachMessage: async ({ partition, message }) => {
        // Converte o buffer da mensagem em objeto JSON
        const evento = JSON.parse(message.value?.toString() || '{}');

        // Verifica se o evento corresponde a lançamento de nova nota
        if (evento.tipo === 'NOVA_NOTA') {
          // Imprime a ação tomada pelo serviço de notificações push
          console.log(`📲 [Push Notification] Disparando alerta para o app mobile: "${evento.mensagemPush}" (Partição: ${partition})`);
        }
      },
    });
  } catch (error) {
    // Registra falhas de conexão ou processamento no console
    console.error('❌ [Notificação] Falha no consumer do Kafka:', error);
  }
}

// Escuta o sinal de interrupção (Ctrl+C) para desconectar com segurança
process.on('SIGINT', async () => {
  // Desconecta o consumidor do broker de forma limpa
  await consumer.stop();
  // Encerra o processo Node.js
  process.exit(0);
});

// Executa a função principal do serviço
run();
