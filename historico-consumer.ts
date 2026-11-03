// Importa a biblioteca do cliente Kafka para Node.js
import { Kafka } from 'kafkajs';

// Configura a conexão com o broker local do Kafka
const kafka = new Kafka({ clientId: 'servico-historico', brokers: ['localhost:9092'] });

// Cria a instância do consumidor associado ao grupo 'historico-group'
const consumer = kafka.consumer({ groupId: 'historico-group' });

// Função principal assíncrona que gerencia o ciclo de vida do consumidor
async function run() {
  try {
    // Conecta o consumidor ao cluster do Kafka
    await consumer.connect();
    // Exibe mensagem informativa de conexão bem-sucedida
    console.log('✅ [Histórico] Conectado ao Kafka com sucesso!');

    // Inscreve o consumidor no tópico 'eventos-academicos' lendo desde o início
    await consumer.subscribe({ topic: 'eventos-academicos', fromBeginning: true });
    // Exibe status de prontidão para receber mensagens
    console.log('🎧 [Histórico] Aguardando mensagens no tópico "eventos-academicos"...\n');

    // Inicia o processamento contínuo de cada mensagem recebida
    await consumer.run({
      // Callback executado para cada mensagem consumida
      eachMessage: async ({ partition, message }) => {
        // Converte o buffer da mensagem em objeto JSON
        const evento = JSON.parse(message.value?.toString() || '{}');
        // Imprime a ação tomada pelo serviço de histórico
        console.log(`📥 [Histórico] Atualizando banco de dados e recalculando média do aluno ${evento.matricula}... (Partição: ${partition}, Nota: ${evento.valor})`);
      },
    });
  } catch (error) {
    // Registra falhas de conexão ou processamento no console
    console.error('❌ [Histórico] Falha no consumer do Kafka:', error);
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
