# Elekto.Mcp.Sql

MCP Server somente leitura para introspecção e consulta de bancos SQL Server 2022+.
Expõe schema, definições de objetos e consultas de dados via protocolo MCP (stdio),
permitindo que o GitHub Copilot (e outros clientes MCP) entendam a estrutura do banco
sem acesso direto a credenciais no código do repositório.

## Ferramentas disponíveis

| Tool | Descrição |
|------|-----------|
| `list_databases` | Bancos registrados na configuração |
| `list_schemas` | Schemas de um banco (excluindo sistema) |
| `list_tables` | Tabelas com schema e contagem aprox. de linhas |
| `list_views` | Views de usuário |
| `list_procedures` | Stored procedures de usuário |
| `list_functions` | Funções de usuário (scalar, inline table, multi-statement table) |
| `get_table_schema` | Colunas, PKs, FKs e índices de uma tabela |
| `get_view_definition` | DDL + colunas de uma view |
| `get_procedure_definition` | Texto CREATE PROCEDURE |
| `get_function_definition` | Texto CREATE FUNCTION |
| `query_table` | SELECT em tabela/view com filtro, ordenação e paginação |

## Configuração

A configuração é feita via variável de ambiente `MCP_SQL_CONNECTIONS`,
com um objeto JSON mapeando nomes para configurações de banco.

### Formato simples (string de conexão direta)

```json
{
  "RiskSystem": "Server=.\\DEV;Database=RiskSystem;Integrated Security=SSPI"
}
```

### Formato completo (com opções)

```json
{
  "RiskSystem": {
    "connection_string": "Server=.\\DEV;Database=RiskSystem;Integrated Security=SSPI",
    "max_query_rows": 10000
  },
  "Relatorios": {
    "connection_string": "Server=.\\PROD;Database=Relatorios;User Id=%{DB_USER};Password=%{DB_PASS}",
    "max_query_rows": 1000
  }
}
```

### Expansão de variáveis de ambiente

Use `%{NOME_DA_VARIAVEL}` dentro da string de conexão para evitar credenciais em texto claro
nos arquivos de configuração. As variáveis são resolvidas no ambiente do processo no momento
da inicialização do servidor.

Exemplo: `%{DB_PASS}` é substituído pelo valor de `$env:DB_PASS`.

Se a variável referenciada não existir, o servidor falha com mensagem de erro explícita.

## Configuração no Visual Studio 2026 (.mcp.json)

Crie ou edite `.mcp.json` na raiz da solution (ou no perfil do usuário para uso global):

```json
{
  "servers": {
    "sql": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["D:\\Tools\\Elekto.Mcp.Sql\\Elekto.Mcp.Sql.dll"],
      "env": {
        "MCP_SQL_CONNECTIONS": "{\"RiskSystem\": {\"connection_string\": \"Server=.\\\\DEV;Database=RiskSystem;Integrated Security=SSPI\"}}"
      }
    }
  }
}
```

Dicas:
- Barras invertidas dentro do JSON precisam de escape duplo (`\\\\` no JSON dentro de JSON).
- Para connection strings com credenciais, prefira variáveis de ambiente:
  ```json
  "env": {
    "MCP_SQL_CONNECTIONS": "{\"DB\": {\"connection_string\": \"...User Id=%{DB_USER};Password=%{DB_PASS}\"}}",
    "DB_USER": "usuario",
    "DB_PASS": "%{SENHA_NO_SISTEMA}"
  }
  ```
  Ou deixe `DB_USER` e `DB_PASS` apenas no ambiente do sistema operacional,
  sem declará-las no `.mcp.json`.

Após salvar o `.mcp.json`, o Copilot reinicia o servidor automaticamente.
As ferramentas ficam desabilitadas por padrão: habilite-as no painel de ferramentas do Copilot Chat.

## Build e publicação

```powershell
cd Elekto.Mcp.Sql
dotnet publish -c Release -o C:\Tools\Elekto.Mcp.Sql
```

Requer .NET 10 instalado na máquina. O diretório publicado tem ~7 MB (dependências NuGet).
Para uso interno, isso é preferível ao self-contained (~81 MB).

## Limites e segurança

- Apenas SELECT em tabelas e views. DML e execução de procedures/funções não são suportados.
- `query_table` constrói o SQL internamente a partir de parâmetros validados. Identificadores
  (tabela, schema, colunas) são validados contra uma expressão regular antes de compor o SQL.
- A cláusula WHERE é aceita como texto livre (necessário para flexibilidade), mas sem
  possibilidade de DML pois o comando é construído como `SELECT TOP n ... FROM [t] WHERE ...`.
- `max_query_rows` limita o número máximo de linhas retornadas por banco (padrão 10.000).
  O parâmetro `top` em `query_table` é sempre limitado a esse valor.
