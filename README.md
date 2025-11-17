# Test pulsar using k6

## Origem do Projeto

Este projeto é um fork de [koolay/xk6-pulsar](https://github.com/koolay/xk6-pulsar). A base original forneceu a funcionalidade essencial para publicar mensagens no Apache Pulsar a partir de testes k6.

---

## Build k6 with extension 

```bash

# install xk6 using go or download(https://github.com/grafana/xk6/releases)
❯ go install go.k6.io/xk6/cmd/xk6@latest

# build k6
❯ xk6 build --with github.com/eduobs/xk6-pulsar=.

```

## Run tests

```bash
# --vus virtual users 
# --duration  how lang continue testing

❯ PULSAR_TOPIC=localtest PULSAR_ADDR=localhost:6650 ./k6 run test_producer.js --duration 10s --vus 2

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: test_producer.js
     output: -

  scenarios: (100.00%) 1 scenario, 2 max VUs, 40s max duration (incl. graceful stop):
           * default: 2 looping VUs for 10s (gracefulStop: 30s)

INFO[0010] teardown!!                                    source=console

running (10.1s), 0/2 VUs, 2142 complete and 0 interrupted iterations
default ✓ [======================================] 2 VUs  10s

     ✓ is send

     █ teardown

     checks.........................: 100.00% ✓ 2142       ✗ 0
     data_received..................: 0 B     0 B/s
     data_sent......................: 0 B     0 B/s
     iteration_duration.............: avg=9.31ms min=102.58µs med=9.19ms max=18.47ms p(90)=10.67ms p(95)=11.69ms
     iterations.....................: 2142    212.791617/s
     pulsar.publish.error.count.....: 0       0/s
     pulsar.publish.message.bytes...: 70 MB   7.0 MB/s
     pulsar.publish.message.count...: 2142    212.791617/s
     vus............................: 2       min=2        max=2
     vus_max........................: 2       min=2        max=2
```

---

## Melhorias Realizadas

As seguintes alterações e melhorias foram implementadas nesta versão para aumentar a robustez e a flexibilidade dos testes:

1.  **Tratamento de Erros Aprimorado**: O tratamento de erros foi modificado para ser menos agressivo. Em vez de usar `log.Fatalf`, que encerrava todo o teste em caso de falha, as funções agora retornam erros. Isso permite que os scripts de teste k6 capturem e tratem falhas individuais (por exemplo, em um único VU) sem interromper a execução completa do teste de carga.

2.  **Correção na Publicação Assíncrona**: Foi corrigido um bug na função de publicação assíncrona (`Publish` com `async: true`). Agora, as métricas (mensagens, bytes e erros) são corretamente reportadas e os erros de publicação são devidamente tratados dentro do callback assíncrono.

3.  **Configuração Flexível**: As configurações do cliente e do produtor Pulsar, que antes eram fixas no código (hardcoded), foram expostas. Agora é possível configurar opções como `ConnectionTimeout`, `CompressionType`, `BatchingMaxMessages`, entre outras, diretamente do script de teste em JavaScript, permitindo cenários de teste muito mais realistas e variados.