using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Transacao.API.BackgroundServices
{
    public class EventosHandler : IHostedService
    {
        private readonly ILogger<EventosHandler> _logger;
        private readonly IConfiguration _configuration;
        private readonly ConsumerConfig _consumerConfig;
        public EventosHandler(ILogger<EventosHandler> logger,
            IConfiguration configuration,
            ConsumerConfig consumerConfig)
        {
            _logger = logger;
            _consumerConfig = consumerConfig;
            _configuration = configuration;
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Consumidor iniciado - Topico: {_configuration["Kafka:Topicos:Transacoes"]}");
            using (var c = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
            {
                if (Convert.ToBoolean(_configuration["Kafka:Topicos:ConsumirMensagensAntigas"])) //If utilizado para consumir mensagens antigas
                    c.Assign(new TopicPartitionOffset(new TopicPartition(_configuration["Kafka:Topicos:Transacoes"],
                                                            Convert.ToInt32(_configuration["Kafka:Topicos:PartitionGroup"])),
                                                     new Offset(Convert.ToInt32(_configuration["Kafka:Topicos:OffSet"]))));
                else
                    c.Subscribe(_configuration["Kafka:Topicos:Transacoes"]);

                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var message = c.Consume(cancellationToken);
                        _logger.LogInformation($"Mensagem Recebida: {message.Message.Value} recebida de {message.TopicPartitionOffset}");
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Consumidor parado - Topico: {_configuration["Kafka:Topicos:Transacoes"]}");
            return Task.CompletedTask;
        }
    }
}
