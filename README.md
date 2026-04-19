# App Financeiro Simples para Celular

Este projeto cria uma interface simples para celular com dois botoes:

- `Enviar PDF`: recebe um extrato bancario em PDF, identifica entradas e saidas e envia os lancamentos para o Google Sheets.
- `Atualizar Pluggy`: consulta a API da Pluggy e envia as transacoes para a sua planilha online.

## Como a planilha foi mapeada

Sua planilha mensal (`JANEIRO`, `FEVEREIRO`, `MARCO`...) ja tem areas prontas para receitas e gastos.
O app preenche os espacos ja formatados da aba mensal:

1. Receitas em `G:I`
2. Gastos variaveis em `L:Q`

As datas dos gastos sao enviadas como texto (`dd/mm/aaaa`) para preservar a aparencia da planilha.

## Antes de usar

1. Suba seu arquivo Excel para o Google Drive e abra como Google Sheets.
2. O projeto ja vem com o `.env` configurado para sua planilha.
3. Salve o JSON da conta de servico em `credentials/google-service-account.json`.
4. Compartilhe a planilha com o e-mail da conta de servico.
5. Preencha as credenciais da Pluggy no `.env` se quiser usar o botao de atualizacao online.

## Instalacao

```powershell
C:\Users\Emerson\AppData\Local\Programs\Python\Python312\python.exe -m pip install -r requirements.txt
```

## Execucao

```powershell
C:\Users\Emerson\AppData\Local\Programs\Python\Python312\python.exe app.py
```

Depois, abra no navegador do celular ou no computador:

- `http://SEU_IP_LOCAL:5000`
- ou `http://127.0.0.1:5000`

## Configuracao pronta neste projeto

- `GOOGLE_SHEETS_SPREADSHEET_ID=1OdVOx46qvUClBqGChrU8OaL1Q5UFrVg3hCwrxAXIEnM`
- `GOOGLE_APPLICATION_CREDENTIALS=credentials/google-service-account.json`
- `GOOGLE_SHEETS_EXPENSE_RANGE_TEMPLATE={MES}!L:Q`

## Observacoes importantes

- A leitura de PDF varia de banco para banco. O parser atual ja tenta capturar linhas no formato `data + descricao + valor`.
- Se o PDF nao puder ser interpretado automaticamente, o sistema cria um lancamento de revisao para voce nao perder o arquivo.
- A integracao da Pluggy depende do `itemId` da conta conectada.
- Esta primeira versao prioriza simplicidade. Podemos evoluir depois para autenticacao, categorias automaticas e publicacao como app instalavel.
