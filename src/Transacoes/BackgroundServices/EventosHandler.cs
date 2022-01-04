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
        private readonly ConsumerConfig _consumerConfig;
        private readonly IConfiguration _configuration;
        public EventosHandler(ILogger<EventosHandler> logger, ConsumerConfig consumerConfig, IConfiguration configuration)
        {
            _logger = logger;
            _consumerConfig = consumerConfig;
            _configuration = configuration;
        }
        public Task StartAsync(CancellationToken cancellationToken)
        {
            using (var c = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
            {
                c.Subscribe(_configuration["Kafka:Topicos:Transacoes"]);
                var cts = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        var message = c.Consume(cts.Token);
                        _logger.LogInformation($"Mensagem: {message.Message.Value} recebida de {message.TopicPartitionOffset}");
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
            return Task.CompletedTask;
        }
    }
}
