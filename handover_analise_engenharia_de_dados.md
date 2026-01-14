# Handover – Analise e Engenharia de Dados | Barra Mansa Alimentos

**Autor:** Joao Lima<br> 
**Departamento:** Controladoria Financeira<br>
**Data:** Janeiro 2026<br>

---

## 1. Linguagens de Programacao

### 1.1. SQL
- [1.1.1. SQL Basico (SELECT, WHERE, FROM)](#111-sql-basico-select-where-from)
- [1.1.2. SQL Intermediario (JOINs, Agregacões, CTEs, Window Functions, UNION)](#112-sql-intermediario-joins-agregacoes-ctes-window-functions-union)
- [1.1.3. SQL Aplicado - COFINS a Recuperar (exemplo real)](#113-sql-aplicado-cofins-a-recuperar-exemplo-real)

### 1.2. Python
- [1.2.1. Sintaxe Basica (tipos, colecões, operadores, controle)](#121-sintaxe-basica-tipos-colecoes-operadores-controle)
- [1.2.2. Recursos da Linguagem (comprehension, decorators, context manager)](#122-recursos-da-linguagem-comprehension-decorators-context-manager)
- [1.2.3. Pandas para Analise Exploratoria](#123-pandas-para-analise-exploratoria)

---

## 2. Conceitos e Ciclo de Vida

### 2.1. Ciclo de Vida da Analise de Dados
- [2.1.1. Etapas do Ciclo (6 etapas)](#211-etapas-do-ciclo-6-etapas)
- [2.1.2. Detalhamento das Etapas](#212-detalhamento-das-etapas)

### 2.2. Arquitetura ELT
- [2.2.1. Visao Geral do Processo](#221-visao-geral-do-processo)
- [2.2.2. Componentes Principais (fontes, EL, transformacao, camadas, visualizacao)](#222-componentes-principais-fontes-el-transformacao-camadas-visualizacao)

---

## 3. Ambiente e Ferramentas

### 3.1. WSL/Linux
- [3.1.1. Navegacao](#311-navegacao)
- [3.1.2. Gerenciamento de Arquivos](#312-gerenciamento-de-arquivos)
- [3.1.3. Gerenciamento de Pacotes](#313-gerenciamento-de-pacotes)

### 3.2. Python (Ambiente)
- [3.2.1. Criar e Ativar Ambiente Virtual](#321-criar-e-ativar-ambiente-virtual)
- [3.2.2. Gerenciar Dependências](#322-gerenciar-dependencias)

### 3.3. VS Code
- [3.3.1. Extensões Recomendadas](#331-extensoes-recomendadas)
- [3.3.2. Atalhos Principais](#332-atalhos-principais)

### 3.4. Git
- [3.4.1. Comandos Basicos](#341-comandos-basicos)
- [3.4.2. Desfazer Alteracões](#342-desfazer-alteracoes)
- [3.4.3. Resolucao de Conflitos](#343-resolucao-de-conflitos)
- [3.4.4. Boas Praticas](#344-boas-praticas)

### 3.5. Fluxo de Desenvolvimento
- [3.5.1. Git → dbt → GitHub (passo a passo)](#351-git-dbt-github-passo-a-passo)

---

## 4. Stack de Dados

### 4.1. AWS
- [4.1.1. Visao Geral](#411-visao-geral)
- [4.1.2. S3 (staging)](#412-s3-staging)
- [4.1.3. EC2 (Airflow)](#413-ec2-airflow)
- [4.1.4. Redshift (DW)](#414-redshift-dw)

### 4.2. dbt
- [4.2.1. Estrutura do Projeto](#421-estrutura-do-projeto)
- [4.2.2. Camadas de Dados](#422-camadas-de-dados)
- [4.2.3. Materializacões](#423-materializacoes)
- [4.2.4. Sources e Refs](#424-sources-e-refs)
- [4.2.5. Testes](#425-testes)
- [4.2.6. Jinja Basico](#426-jinja-basico)
- [4.2.7. Comandos dbt](#427-comandos-dbt)

### 4.3. Airflow
- [4.3.1. Conceitos Essenciais](#431-conceitos-essenciais)
- [4.3.2. Operadores](#432-operadores)
- [4.3.3. Anatomia de uma DAG](#433-anatomia-de-uma-dag)
- [4.3.4. Hooks e Conexões](#434-hooks-e-conexoes)
- [4.3.5. Recursos Intermediarios](#435-recursos-intermediarios)
- [4.3.6. Airflow na Barra Mansa](#436-airflow-na-barra-mansa)
- [4.3.7. Como Adicionar Nova Tabela](#437-como-adicionar-nova-tabela)
- [4.3.8. Operacao](#438-operacao)

### 4.4. Power BI
- [4.4.1. Conexao com Fontes](#441-conexao-com-fontes)
- [4.4.2. Modelagem de Dados](#442-modelagem-de-dados)
- [4.4.3. DAX Intermediario](#443-dax-intermediario)
- [4.4.4. Design de Layouts](#444-design-de-layouts)
- [4.4.5. Criacao de Visuais](#445-criacao-de-visuais)
- [4.4.6. Interatividade](#446-interatividade)
- [4.4.7. Boas Praticas](#447-boas-praticas)
- [4.4.8. Exemplos Contextualizados](#448-exemplos-contextualizados)

---

## 5. Qualidade e Validacao

### 5.1. Validacao Cruzada
- [5.1.1. Importância](#511-importancia)
- [5.1.2. Princípios](#512-principios)
- [5.1.3. Recomendacao](#513-recomendacao)

---

## 6. Pratica

### 6.1. Exercícios SQL Analíticos
- [6.1.1. Basico (SELECT + WHERE)](#611-basico-select-where)
- [6.1.2. JOINs](#612-joins)
- [6.1.3. Agregacões](#613-agregacoes)
- [6.1.4. CTEs](#614-ctes)
- [6.1.5. Window Functions](#615-window-functions)
- [6.1.6. UNION + Subconsultas + DML](#616-union-subconsultas-dml)

### 6.2. Exercícios Python para Analise de Dados
- [6.2.1. Sintaxe Basica](#621-sintaxe-basica)
- [6.2.2. Recursos da Linguagem](#622-recursos-da-linguagem)
- [6.2.3. Pandas para Analise Exploratoria](#623-pandas-para-analise-exploratoria)

### 6.3. Exercícios DAX
- [6.3.1. Medidas Basicas com Filtros](#631-medidas-basicas-com-filtros)
- [6.3.2. Comparacões Temporais](#632-comparacoes-temporais)
- [6.3.3. Totalizadores e Contextos](#633-totalizadores-e-contextos)
- [6.3.4. Indicadores e Logica Condicional](#634-indicadores-e-logica-condicional)
- [6.3.5. Logica Avancada e Iteracao](#635-logica-avancada-e-iteracao)

### 6.4. Exercícios Linguagem M (Power Query)
- [6.4.1. Transformacões Basicas](#641-transformacoes-basicas)
- [6.4.2. Transformacões Intermediarias](#642-transformacoes-intermediarias)
- [6.4.3. Analise de Pipeline Completo](#643-analise-de-pipeline-completo)

---

# 1. Linguagens de Programacao

## 1.1. SQL

Guia visual e pratico para construcao de consultas SQL no ambiente Barra Mansa Alimentos.

### 1.1.1. SQL Basico (SELECT, WHERE, FROM)

#### Estrutura de uma Consulta SQL

O processo completo de construcao de uma query SQL segue três passos sequenciais:


- Liste as colunas que deseja exibir
- Use o alias da tabela (N.) antes de cada coluna

**Exemplo:**
```sql
SELECT N.codemp, N.codfil, N.numnfi, N.datent
```

### 2º Passo: FROM - DE ONDE vêm os dados

- Especifique a tabela e crie um alias para facilitar referências

**Exemplo:**
```sql
FROM e660nfc N
-- e660nfc = tabela | N = alias/apelido
```

### 3º Passo: WHERE - QUAIS dados filtrar

- Defina condicões para limitar os resultados
- Use AND/OR para combinar múltiplas condicões

**Exemplo:**
```sql
WHERE N.codemp = 1 
  AND N.datent >= '20250615' 
  AND N.vlrtot > 1000
```

### Consulta Completa de Exemplo

```sql
SELECT N.codemp, N.codfil, N.numnfi, N.datent
FROM e660nfc N
WHERE N.codemp = 1 
  AND N.datent >= '20250615' 
  AND N.vlrtot > 1000
```

**Ambiente de execucao:** SSMS (SQL Server Management Studio) no Banco de Dados `sapiens_prod`

---

### 1.1.2. SQL Intermediario (JOINs, Agregacões, CTEs, Window Functions, UNION)

---

#### JOINs - Combinando Dados de Múltiplas Tabelas

##### LEFT JOIN
Retorna todos os registros da tabela principal (esquerda) e os correspondentes da tabela secundaria. Se nao houver correspondência, retorna NULL.

**Exemplo do contexto:**
```sql
FROM E640LCT L
LEFT JOIN e045pla P
    ON P.codemp = L.codemp
   AND P.ctared = L.ctadeb
```
- Busca **todos** os lancamentos da E640LCT
- Traz descricões do plano de contas quando houver correspondência
- Se nao houver conta correspondente, P.descta sera NULL

##### INNER JOIN
Retorna apenas registros que têm correspondência em ambas as tabelas.
```sql
FROM E640LCT L
INNER JOIN e045pla P
    ON P.codemp = L.codemp
   AND P.ctared = L.ctadeb
```
- Retornaria apenas lancamentos que têm conta no plano de contas

##### RIGHT JOIN
Retorna todos os registros da tabela secundaria (direita) e os correspondentes da tabela principal.
```sql
FROM E640LCT L
RIGHT JOIN e045pla P
    ON P.codemp = L.codemp
   AND P.ctared = L.ctadeb
```

##### FULL OUTER JOIN
Retorna todos os registros quando ha correspondência em qualquer uma das tabelas.
```sql
FROM E640LCT L
FULL OUTER JOIN e045pla P
    ON P.codemp = L.codemp
   AND P.ctared = L.ctadeb
```

---

#### UNION - Combinando Resultados de Múltiplas Consultas

Combina os resultados de duas ou mais queries em um único conjunto de dados.

##### UNION vs UNION ALL

**UNION** - Remove duplicatas automaticamente:
```sql
SELECT ctadeb, vlrlct FROM E640LCT WHERE ctadeb = '10530'
UNION
SELECT ctacre, vlrlct FROM E640LCT WHERE ctacre = '10530'
```

**UNION ALL** - Mantém todas as linhas, incluindo duplicatas (mais rapido):
```sql
SELECT ctadeb, vlrlct FROM E640LCT WHERE ctadeb = '10530'
UNION ALL
SELECT ctacre, vlrlct FROM E640LCT WHERE ctacre = '10530'
```

##### Exemplo Pratico - COFINS a Recuperar
```sql
-- Lancamentos a CRÉDITO (conta 10.530 recebendo)
SELECT
    L.codemp,
    L.codfil,
    L.numlct,
    CAST(L.datlct AS DATE) AS datlct,
    (L.vlrlct * -1) AS vlrlct,  -- Inverte o sinal
    'CRÉDITO' AS tipo_lancamento
FROM E640LCT L
WHERE L.ctacre = '10530'
  AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
  AND L.sitlct = 2

UNION

-- Lancamentos a DÉBITO (conta 10.530 pagando)
SELECT
    L.codemp,
    L.codfil,
    L.numlct,
    CAST(L.datlct AS DATE) AS datlct,
    L.vlrlct,  -- Mantém o sinal positivo
    'DÉBITO' AS tipo_lancamento
FROM E640LCT L
WHERE L.ctadeb = '10530'
  AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
  AND L.sitlct = 2
```

**Por que usar UNION?**
- Conta contabil pode aparecer tanto no débito quanto no crédito
- UNION consolida ambas as movimentacões em uma única visao
- Permite padronizar valores (invertendo sinal quando necessario)

**Regras do UNION:**
- Todas as queries devem ter o **mesmo número de colunas**
- Colunas correspondentes devem ter **tipos de dados compatíveis**
- Nomes das colunas vêm da **primeira query**

---

#### Agregacões - Sumarizando Dados

Funcões que calculam valores sobre grupos de registros.

##### Funcões de Agregacao Principais
```sql
-- Soma total de COFINS por filial
SELECT 
    L.codfil,
    SUM(L.vlrlct) AS total_cofins,
    COUNT(*) AS qtd_lancamentos,
    COUNT(DISTINCT L.numlct) AS qtd_lancamentos_unicos,
    AVG(L.vlrlct) AS media_valor,
    MAX(L.vlrlct) AS maior_valor,
    MIN(L.vlrlct) AS menor_valor
FROM E640LCT L
WHERE L.ctacre = '10530'
  AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
GROUP BY L.codfil
```

**Funcões disponíveis:**
- `SUM()` - Soma valores
- `COUNT()` - Conta registros
- `COUNT(DISTINCT)` - Conta valores únicos
- `AVG()` - Calcula média
- `MAX()` / `MIN()` - Valores maximo/mínimo

##### HAVING - Filtro Apos Agregacao
```sql
SELECT 
    L.codfil,
    SUM(L.vlrlct) AS total_cofins,
    COUNT(*) AS qtd_lancamentos
FROM E640LCT L
WHERE L.ctacre = '10530'
GROUP BY L.codfil
HAVING SUM(L.vlrlct) > 10000  -- Filtra apos agregacao
```

**Diferenca entre WHERE e HAVING:**
- `WHERE` - Filtra **antes** da agregacao (registros individuais)
- `HAVING` - Filtra **depois** da agregacao (grupos)

---

#### CTEs (Common Table Expressions) - Consultas Temporarias

Cria "tabelas temporarias" nomeadas para organizar queries complexas de forma legível.

##### Sintaxe Basica
```sql
WITH nome_cte AS (
    SELECT coluna1, coluna2
    FROM tabela
    WHERE condicao
)
SELECT * FROM nome_cte
```

##### Exemplo Pratico - Analise de COFINS
```sql
WITH cofins_debito AS (
    -- Totaliza lancamentos a débito
    SELECT 
        codemp, 
        codfil, 
        SUM(vlrlct) AS total_debito,
        COUNT(*) AS qtd_debito
    FROM E640LCT
    WHERE ctadeb = '10530'
      AND datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
      AND sitlct = 2
    GROUP BY codemp, codfil
),
cofins_credito AS (
    -- Totaliza lancamentos a crédito
    SELECT 
        codemp, 
        codfil, 
        SUM(vlrlct) AS total_credito,
        COUNT(*) AS qtd_credito
    FROM E640LCT
    WHERE ctacre = '10530'
      AND datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
      AND sitlct = 2
    GROUP BY codemp, codfil
)
-- Consolida débito e crédito
SELECT 
    COALESCE(D.codfil, C.codfil) AS codfil,
    COALESCE(D.total_debito, 0) AS total_debito,
    COALESCE(C.total_credito, 0) AS total_credito,
    (COALESCE(D.total_debito, 0) - COALESCE(C.total_credito, 0)) AS saldo_liquido,
    COALESCE(D.qtd_debito, 0) AS lancamentos_debito,
    COALESCE(C.qtd_credito, 0) AS lancamentos_credito
FROM cofins_debito D
FULL OUTER JOIN cofins_credito C
    ON D.codemp = C.codemp
   AND D.codfil = C.codfil
```

**Vantagens das CTEs:**
- Codigo mais legível e organizado
- Reutilizacao de resultados intermediarios
- Substituicao elegante de subqueries complexas
- Facilita debugging (pode testar cada CTE separadamente)
- **Alternativa ao UNION** quando você quer manter os dados separados para analise

##### CTEs Recursivas
```sql
WITH hierarquia_contas AS (
    -- Âncora: contas de nível superior
    SELECT ctared, descta, ctasup, 1 AS nivel
    FROM e045pla
    WHERE ctasup IS NULL
    
    UNION ALL
    
    -- Recursao: contas subordinadas
    SELECT p.ctared, p.descta, p.ctasup, h.nivel + 1
    FROM e045pla p
    INNER JOIN hierarquia_contas h
        ON p.ctasup = h.ctared
)
SELECT * FROM hierarquia_contas
ORDER BY nivel, ctared
```

---

#### Window Functions - Calculos Analíticos

Realizam calculos mantendo o detalhamento de cada linha (diferente do GROUP BY que agrupa).

##### Sintaxe Basica
```sql
funcao_janela() OVER (
    [PARTITION BY coluna]
    [ORDER BY coluna]
    [ROWS/RANGE BETWEEN ... AND ...]
)
```

##### Funcões de Ranking
```sql
SELECT 
    L.numlct,
    L.datlct,
    L.codfil,
    L.vlrlct,
    -- Numeracao sequencial única
    ROW_NUMBER() OVER (ORDER BY L.vlrlct DESC) AS ranking_geral,
    -- Ranking com empates (pula números)
    RANK() OVER (ORDER BY L.vlrlct DESC) AS rank_com_pulo,
    -- Ranking com empates (sem pular números)
    DENSE_RANK() OVER (ORDER BY L.vlrlct DESC) AS rank_denso,
    -- Ranking por filial
    ROW_NUMBER() OVER (PARTITION BY L.codfil ORDER BY L.vlrlct DESC) AS ranking_filial
FROM E640LCT L
WHERE L.ctacre = '10530'
  AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
```

##### Funcões de Agregacao com OVER
```sql
SELECT 
    L.numlct,
    L.datlct,
    L.codfil,
    L.vlrlct,
    -- Soma acumulada ordenada por data
    SUM(L.vlrlct) OVER (ORDER BY L.datlct) AS soma_acumulada_geral,
    -- Soma acumulada por filial
    SUM(L.vlrlct) OVER (PARTITION BY L.codfil ORDER BY L.datlct) AS soma_acumulada_filial,
    -- Média movel dos últimos 3 lancamentos
    AVG(L.vlrlct) OVER (ORDER BY L.datlct ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS media_movel_3,
    -- Total geral (sem particao)
    SUM(L.vlrlct) OVER () AS total_geral,
    -- Percentual do total
    (L.vlrlct * 100.0 / SUM(L.vlrlct) OVER ()) AS percentual_total
FROM E640LCT L
WHERE L.ctacre = '10530'
  AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
```

##### Funcões de Acesso a Linhas
```sql
SELECT 
    L.numlct,
    L.datlct,
    L.vlrlct,
    -- Valor do lancamento anterior
    LAG(L.vlrlct, 1) OVER (ORDER BY L.datlct) AS valor_anterior,
    -- Valor do proximo lancamento
    LEAD(L.vlrlct, 1) OVER (ORDER BY L.datlct) AS valor_proximo,
    -- Diferenca em relacao ao anterior
    L.vlrlct - LAG(L.vlrlct, 1) OVER (ORDER BY L.datlct) AS variacao,
    -- Primeiro valor do período
    FIRST_VALUE(L.vlrlct) OVER (ORDER BY L.datlct) AS primeiro_valor,
    -- Último valor do período
    LAST_VALUE(L.vlrlct) OVER (ORDER BY L.datlct ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ultimo_valor
FROM E640LCT L
WHERE L.ctacre = '10530'
  AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
```

##### Clausulas de Window Frame
```sql
SELECT 
    L.datlct,
    L.vlrlct,
    -- Soma dos últimos 7 dias (incluindo hoje)
    SUM(L.vlrlct) OVER (
        ORDER BY L.datlct 
        RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
    ) AS soma_7_dias,
    -- Média das 5 linhas anteriores até a atual
    AVG(L.vlrlct) OVER (
        ORDER BY L.datlct 
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS media_movel_5,
    -- Total da particao inteira
    SUM(L.vlrlct) OVER (
        PARTITION BY L.codfil
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS total_filial
FROM E640LCT L
WHERE L.ctacre = '10530'
```

**Diferenca entre ROWS e RANGE:**
- `ROWS` - Define janela por **número de linhas** físicas
- `RANGE` - Define janela por **valores** (útil para datas)

---

#### Aplicacao Pratica no Contexto

A query de COFINS a Recuperar demonstra a aplicacao desses conceitos:

1. **LEFT JOIN** - Enriquece dados com descricões do plano de contas
2. **UNION** - Consolida lancamentos de débito e crédito em visao única
3. **Agregacao** - Poderia totalizar movimentacões por período/filial
4. **CTEs** - Alternativa para separar logica de débito/crédito antes de consolidar
5. **Window Functions** - Analise de evolucao temporal e calculo de saldos acumulados

##### Exemplo Completo Combinando Técnicas
```sql
WITH lancamentos_consolidados AS (
    -- CTE: Consolida débito e crédito com UNION
    SELECT
        L.codemp,
        L.codfil,
        L.numlct,
        CAST(L.datlct AS DATE) AS datlct,
        (L.vlrlct * -1) AS vlrlct,
        'CRÉDITO' AS tipo,
        L.cpllct
    FROM E640LCT L
    WHERE L.ctacre = '10530'
      AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
      AND L.sitlct = 2
    
    UNION ALL
    
    SELECT
        L.codemp,
        L.codfil,
        L.numlct,
        CAST(L.datlct AS DATE) AS datlct,
        L.vlrlct,
        'DÉBITO' AS tipo,
        L.cpllct
    FROM E640LCT L
    WHERE L.ctadeb = '10530'
      AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
      AND L.sitlct = 2
)
-- Query principal com JOINs, Agregacões e Window Functions
SELECT 
    LC.codfil,
    LC.datlct,
    LC.tipo,
    COUNT(*) AS qtd_lancamentos,
    SUM(LC.vlrlct) AS total_dia,
    -- Window Function: Saldo acumulado por filial
    SUM(SUM(LC.vlrlct)) OVER (
        PARTITION BY LC.codfil 
        ORDER BY LC.datlct
    ) AS saldo_acumulado,
    -- Window Function: Média movel 7 dias
    AVG(SUM(LC.vlrlct)) OVER (
        PARTITION BY LC.codfil 
        ORDER BY LC.datlct 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS media_movel_7_dias
FROM lancamentos_consolidados LC
GROUP BY LC.codfil, LC.datlct, LC.tipo
ORDER BY LC.codfil, LC.datlct, LC.tipo
```

---

### 1.1.3. SQL Aplicado - COFINS a Recuperar (exemplo real)

```sql
-- =========================================================
-- Query: COFINS a Recuperar – Visao Contabil (Conta 10.530)
-- Descricao: Consulta lancamentos contabeis da conta 10.530 (COFINS a Recuperar)
--             excluindo lotes específicos ja processados
--             Uniao de lancamentos a débito e crédito
-- =========================================================

SELECT
    L.codemp,
    L.codfil,
    L.numlct,
    CAST(L.datlct AS DATE) AS datlct,
    L.ctadeb,
    P.descta AS P_descta,
    L.ctacre,
    A.descta AS A_descta,
    (L.vlrlct * -1) AS vlrlct,
    L.cpllct,
    L.orilct,
    L.numlot,
    L.temaux,
    L.codusu,
    N.numnfi
FROM E640LCT L
LEFT JOIN e045pla P
    ON P.codemp = L.codemp
   AND P.ctared = L.ctadeb
LEFT JOIN e045pla A
    ON A.codemp = L.codemp
   AND A.ctared = L.ctacre
LEFT JOIN E644LNF N
    ON N.codemp = L.codemp
   AND N.numlct = L.numlct
WHERE
    L.ctacre = '10530'
    AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
    AND L.codfil = 4
    AND L.numlot NOT IN ('52660', '52751', '52826')
    AND L.sitlct = 2

UNION

SELECT
    L.codemp,
    L.codfil,
    L.numlct,
    CAST(L.datlct AS DATE) AS datlct,
    L.ctadeb,
    P.descta AS P_descta,
    L.ctacre,
    A.descta AS A_descta,
    L.vlrlct,
    L.cpllct,
    L.orilct,
    L.numlot,
    L.temaux,
    L.codusu,
    N.numnfi
FROM E640LCT L
LEFT JOIN e045pla P
    ON P.codemp = L.codemp
   AND P.ctared = L.ctadeb
LEFT JOIN e045pla A
    ON A.codemp = L.codemp
   AND A.ctared = L.ctacre
LEFT JOIN E644LNF N
    ON N.codemp = L.codemp
   AND N.numlct = L.numlct
WHERE
    L.ctadeb = '10530'
    AND L.datlct BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
    AND L.codfil = 4
    AND L.numlot NOT IN ('52660', '52751', '52826')
    AND L.sitlct = 2;

```

**Ambiente de execucao:** SSMS (SQL Server Management Studio) no Banco de Dados `sapiens_prod`

---

### Objetivo da Consulta

Extrair todos os lancamentos contabeis da conta **10.530** (COFINS a Recuperar) nos últimos 90 dias da filial 4, excluindo lotes específicos ja processados.

### Estrutura da Query

A consulta é dividida em duas partes unidas por `UNION`:

#### Parte 1 - Lancamentos a Crédito

- Busca quando a conta 10.530 esta em `ctacre` (lado crédito)
- Inverte o sinal do valor (`vlrlct * -1`) para padronizar visualizacao
- Retorna movimentos de entrada/crédito de COFINS

#### Parte 2 - Lancamentos a Débito

- Busca quando a conta 10.530 esta em `ctadeb` (lado débito)
- Mantém o valor positivo original
- Retorna movimentos de saída/débito de COFINS

### Tabelas Utilizadas

| Tabela | Descricao |
|--------|-----------|
| `E640LCT` | Lancamentos contabeis (tabela principal) |
| `e045pla` | Plano de contas (descricao das contas débito e crédito) |
| `E644LNF` | Vinculacao com números de notas fiscais |

### Filtros Aplicados

| Filtro | Valor |
|--------|-------|
| Período | Últimos 90 dias |
| Filial | 4 |
| Lotes excluídos | 52660, 52751, 52826 |
| Situacao do lancamento | 2 (contabilizado) |

### Resultado Esperado

Visao consolidada dos movimentos de COFINS a Recuperar com valores, descricões das contas, complementos historicos e vínculos com notas fiscais.

---

## 1.2. Python

Guia rapido com a base mínima da linguagem.

### 1.2.1. Sintaxe Basica (tipos, colecões, operadores, controle) {#121-sintaxe-basica-tipos-colecões-operadores-controle}


#### Tipos de Dados

```python
# String (texto)
nome = "Joao"
query = 'SELECT * FROM tabela'

# Inteiro e Float (números)
quantidade = 100
preco = 49.90

# Boolean (verdadeiro/falso)
ativo = True
processado = False

# None (ausência de valor)
resultado = None
```

---

#### Colecões

**Quando usar cada uma:**
- **Lista** → quando a ordem importa e pode mudar
- **Dicionario** → quando precisa buscar por chave
- **Tupla** → quando nao pode mudar (dados fixos)

#### Lista
Sequência ordenada e mutavel.
```python
frutas = ["maca", "banana", "laranja"]
frutas.append("uva")          # Adiciona
frutas[0]                     # Acessa: "maca"
len(frutas)                   # Tamanho: 4
```

#### Dicionario
Pares chave-valor.
```python
pessoa = {
    "nome": "Joao",
    "idade": 30,
    "ativo": True
}
pessoa["nome"]                # Acessa: "Joao"
pessoa["cargo"] = "Analista"  # Adiciona chave
pessoa.keys()                 # Lista chaves
pessoa.values()               # Lista valores
```

#### Tupla
Sequência imutavel.
```python
coordenadas = (10, 20)
host, porta = ("localhost", 5432)  # Desempacotamento
```

---

### Operadores

```python
# Aritméticos
soma = 10 + 5         # 15
divisao = 10 / 3      # 3.333
inteiro = 10 // 3     # 3
resto = 10 % 3        # 1

# Comparacao
10 == 10              # True
10 != 5               # True
10 > 5                # True
10 <= 10              # True

# Logicos
True and False        # False
True or False         # True
not True              # False
```

---

### Estruturas de Controle

#### Condicional
```python
idade = 25

if idade >= 18:
    print("Maior de idade")
elif idade >= 12:
    print("Adolescente")
else:
    print("Crianca")
```

#### Loop For
```python
# Iterando lista
frutas = ["maca", "banana", "laranja"]
for fruta in frutas:
    print(fruta)

# Iterando com índice
for i, fruta in enumerate(frutas):
    print(f"{i}: {fruta}")

# Range
for i in range(5):       # 0, 1, 2, 3, 4
    print(i)
```

#### Loop While
```python
contador = 0
while contador < 3:
    print(contador)
    contador += 1
```

#### Controle de Loop
```python
for i in range(10):
    if i == 3:
        continue      # Pula para proxima iteracao
    if i == 7:
        break         # Sai do loop completamente
    print(i)          # Imprime: 0, 1, 2, 4, 5, 6
```

---

### Funcões

#### Definir e Chamar
```python
def saudacao(nome):
    return f"Ola, {nome}!"

mensagem = saudacao("Joao")   # "Ola, Joao!"
```

#### Parâmetros com Valor Padrao
```python
def conectar(host, porta=5432):
    return f"{host}:{porta}"

conectar("localhost")         # "localhost:5432"
conectar("localhost", 3306)   # "localhost:3306"
```

#### Múltiplos Retornos
```python
def dividir(a, b):
    quociente = a // b
    resto = a % b
    return quociente, resto

q, r = dividir(10, 3)         # q=3, r=1
```

---

#### Tratamento de Erros

**Quando usar:** Evita que o script pare por erros previsíveis (conexao, arquivo nao encontrado, divisao por zero).

```python
try:
    resultado = 10 / 0
except ZeroDivisionError:
    print("Erro: divisao por zero")
except Exception as e:
    print(f"Erro: {e}")
finally:
    print("Sempre executa")  # Limpeza, fechar conexões
```

---

#### Manipulacao de Strings

```python
texto = "  Ola Mundo  "

texto.strip()                 # "Ola Mundo"
texto.lower()                 # "  ola mundo  "
texto.upper()                 # "  OLa MUNDO  "
texto.replace("Mundo", "Python")

# f-strings (formatacao)
nome = "Joao"
idade = 30
f"Nome: {nome}, Idade: {idade}"

# Split e Join
"a,b,c".split(",")            # ["a", "b", "c"]
",".join(["a", "b", "c"])     # "a,b,c"
```

---

#### Bibliotecas Essenciais

##### Importacao
```python
import os                           # Modulo inteiro
from datetime import datetime       # Funcao específica
import pandas as pd                 # Com alias
```

##### Bibliotecas Comuns
| Biblioteca | Uso |
|------------|-----|
| `os` | Variaveis de ambiente, caminhos |
| `datetime` | Manipulacao de datas |
| `json` | Leitura/escrita de JSON |
| `logging` | Logs de execucao |
| `pandas` | Manipulacao de dados |
| `boto3` | Integracao AWS (S3) |

##### Exemplo: datetime
```python
from datetime import datetime, timedelta

agora = datetime.now()
ontem = agora - timedelta(days=1)
formatado = agora.strftime("%Y-%m-%d")   # "2025-01-13"
```

##### Exemplo: os
```python
import os

# Variaveis de ambiente
usuario = os.getenv("DB_USER", "default")

# Caminhos
caminho = os.path.join("pasta", "arquivo.txt")
```

---


### 1.2.2. Recursos da Linguagem (comprehension, decorators, context manager) {#122-recursos-da-linguagem-comprehension-decorators-context-manager}


> Os topicos a seguir aparecem frequentemente em codigo de DAGs e scripts de extracao.

---

#### Comprehension

**Quando usar:** Transformar ou filtrar uma lista/dicionario de forma compacta.

```python
# List comprehension - criar lista transformada
numeros = [1, 2, 3, 4, 5]
dobros = [x * 2 for x in numeros]           # [2, 4, 6, 8, 10]

# Com filtro - criar lista filtrada
pares = [x for x in numeros if x % 2 == 0]  # [2, 4]

# Dict comprehension
nomes = ["ana", "bob"]
tamanhos = {nome: len(nome) for nome in nomes}  # {"ana": 3, "bob": 3}
```

**Exemplo real (extrair nomes de colunas do cursor):**
```python
colunas = [desc[0] for desc in cursor.description]
```

---

#### Context Manager (with)

**Quando usar:** Abrir arquivos, conexões ou recursos que precisam ser fechados.

```python
# Arquivo - fecha automaticamente ao sair do bloco
with open("dados.csv", "r") as f:
    conteudo = f.read()

# Múltiplos recursos
with open("entrada.txt") as entrada, open("saida.txt", "w") as saida:
    saida.write(entrada.read())
```

**Por que usar:** Evita esquecer de fechar arquivos/conexões, mesmo se der erro.

---

#### Decorators

**O que é:** Marcador (`@`) que modifica o comportamento de uma funcao.

```python
@task
def extrair_dados():
    pass
```

**Na pratica:** Você vai *ler* e *usar* decorators prontos, nao criar. Quando vir `@algo` antes de uma funcao, saiba que ela esta sendo "decorada" com comportamento extra.

---

#### Argumentos `**kwargs`

**O que é:** Captura argumentos nomeados extras como dicionario.

```python
def funcao(obrigatorio, **kwargs):
    print(obrigatorio)
    print(kwargs)        # Dicionario com argumentos extras

funcao("valor", nome="Joao", idade=30)
# Saída:
# valor
# {"nome": "Joao", "idade": 30}
```

**Uso comum (acessar contexto do Airflow):**
```python
def minha_task(**context):
    data_execucao = context["ds"]
    params = context["params"]
```

---

#### Type Hints

**O que é:** Indica tipos esperados. Nao obriga, apenas documenta.

```python
def somar(a: int, b: int) -> int:
    return a + b

def processar(nome: str, ativo: bool = True) -> dict:
    return {"nome": nome, "ativo": ativo}
```

**Tipos comuns:** `str`, `int`, `float`, `bool`, `list`, `dict`, `Optional[str]`

---

#### Unpacking

**Quando usar:** Extrair valores de tuplas, listas ou dicionarios em variaveis separadas.

```python
# Tupla/Lista
coordenadas = (10, 20)
x, y = coordenadas                # x=10, y=20

# Em loop com tuplas
pares = [("a", 1), ("b", 2)]
for letra, numero in pares:
    print(f"{letra}: {numero}")

# Dicionario (.items())
config = {"host": "localhost", "porta": 5432}
for chave, valor in config.items():
    print(f"{chave} = {valor}")
```

---


### 1.2.3. Pandas para Analise Exploratoria {#123-pandas-para-analise-exploratoria}


> Biblioteca para analise e manipulacao de dados tabulares.

---

#### Criar DataFrame

```python
import pandas as pd

# De dicionario
df = pd.DataFrame({
    "nome": ["Ana", "Bob", "Carol"],
    "idade": [25, 30, 28],
    "salario": [5000, 6000, 5500]
})

# De arquivo
df = pd.read_csv("dados.csv")
df = pd.read_excel("dados.xlsx")
```

---

#### Conhecer os Dados

```python
df.head()              # Primeiras 5 linhas
df.tail()              # Últimas 5 linhas
df.shape               # (linhas, colunas)
df.columns             # Nomes das colunas
df.dtypes              # Tipos de cada coluna
df.info()              # Resumo geral
df.describe()          # Estatísticas numéricas
```

---

#### Selecionar e Filtrar

```python
# Selecao
df["nome"]             # Uma coluna (Series)
df[["nome", "idade"]]  # Múltiplas colunas (DataFrame)
df.iloc[0]             # Primeira linha por índice
df.iloc[0:3]           # Linhas 0, 1, 2

# Filtros
df[df["idade"] > 25]                              # Idade maior que 25
df[df["nome"] == "Ana"]                           # Nome igual a Ana
df[(df["idade"] > 25) & (df["salario"] > 5000)]   # Múltiplas condicões
```

---

#### Agregar

```python
# Valores únicos
df["salario"].sum()           # Soma
df["salario"].mean()          # Média
df["salario"].max()           # Maximo
df["salario"].min()           # Mínimo
df["nome"].count()            # Contagem
df["nome"].nunique()          # Quantidade de valores únicos
df["cargo"].value_counts()    # Frequência de cada valor

# Group By
df.groupby("departamento")["salario"].mean()      # Média por grupo
df.groupby("departamento").agg({
    "salario": "mean",
    "nome": "count"
})
```

---

#### Tratar Nulos

```python
df.isnull().sum()         # Conta nulos por coluna
df.dropna()               # Remove linhas com nulo
df.fillna(0)              # Substitui nulos por 0
```

---

#### Exportar

```python
df.to_csv("saida.csv", index=False)
df.to_excel("saida.xlsx", index=False)
```

---

##### Teste seu Conhecimento

**O que esse codigo retorna?**

```python
dados = [
    {"nome": "Ana", "ativo": True},
    {"nome": "Bob", "ativo": False},
    {"nome": "Carol", "ativo": True}
]

resultado = [x["nome"] for x in dados if x["ativo"]]
```

<details>
<summary>Ver resposta</summary>

```python
["Ana", "Carol"]
```
Explicacao: List comprehension que filtra apenas os dicionarios onde `ativo` é `True` e extrai o valor de `"nome"`.

</details>

---

# 2. Conceitos e Ciclo de Vida

## 2.1. Ciclo de Vida da Analise de Dados

Framework conceitual para o processo de analise de dados - Pipeline Auditavel End-to-End.

### 2.1.1. Etapas do Ciclo (6 etapas) {#211-etapas-do-ciclo-6-etapas}


```
┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
│ 1. Definicao   │   │ 2. Analise     │   │ 3. Preparacao  │   │ 4. Modelagem   │   │ 5. Validacao   │   │ 6. Power BI    │
│ do Problema    │──▶│ Exploratoria   │──▶│ e Transformacao│──▶│ Dimensional    │──▶│ de Integridade │──▶│                │
│ de Negocio     │   │                │   │                │   │                │   │                │   │                │
└────────────────┘   └────────────────┘   └────────────────┘   └────────────────┘   └────────────────┘   └────────────────┘
     │                    │                    │                    │                    │                    │
     ▼                    ▼                    ▼                    ▼                    ▼                    ▼
 • Objetivos         • Mapeamento         • Staging Layer      • Marts Layer        • Totalizacao       • Modelagem 1:N
 • KPIs              SQL Server           dbt                  dbt                  Tripla              • DAX
 • Stakeholders      • Cardinalidades     • Padronizacao       • Fatos              • Origem → dbt      • Dashboards
                     • Anomalias          • Limpeza            • Dimensões          → SQL Server
                                          • Joins              • Star Schema
                                          • Testes
```


### 2.1.2. Detalhamento das Etapas {#212-detalhamento-das-etapas}


| Etapa | Descricao | Ferramentas/Atividades |
|-------|-----------|------------------------|
| **1. Definicao do Problema** | Identificar a pergunta de negocio | Objetivos, KPIs, Stakeholders |
| **2. Analise Exploratoria** | Entender os dados disponíveis | Mapeamento SQL Server, Cardinalidades, Anomalias |
| **3. Preparacao e Transformacao** | Limpar e padronizar dados | Staging Layer dbt, Padronizacao, Limpeza, Joins, Testes |
| **4. Modelagem Dimensional** | Criar modelo analítico | Marts Layer dbt, Fatos, Dimensões, Star Schema |
| **5. Validacao de Integridade** | Garantir qualidade | Totalizacao Tripla: Origem → dbt → SQL Server |
| **6. Power BI** | Visualizar e entregar | Modelagem 1:N, DAX, Dashboards |

---

## 2.2. Arquitetura ELT

Documentacao da arquitetura de dados implementada na Barra Mansa Alimentos.

### 2.2.1. Visao Geral do Processo {#221-visao-geral-do-processo}


O pipeline de dados esta estruturado para processar informacões dos sistemas corporativos e industriais (ERP Sapiens e Sistema AIS) até a disponibilizacao em dashboards no Power BI Service.

#### Arquitetura do Pipeline

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────────────────────────┐     ┌─────────────────┐
│   FONTES    │     │  EXTRAcaO &      │     │           DATA WAREHOUSE                │     │  VISUALIZAcaO   │
│             │     │  CARGA (EL)      │     │          Amazon Redshift                │     │                 │
│ ERP Sapiens │────▶│                  │     │                                         │     │  Power BI       │
│ Sistema AIS │     │ Scripts Python   │────▶│  Raw    → Source  → Staging → Marts    │────▶│  Service        │
│             │     │ Apache Airflow   │     │  Layer    Layer     Layer     Layer     │     │  Dashboards     │
└─────────────┘     └──────────────────┘     │  (Dados   (Padro-   (Limpeza) (Dimensões│     │  Corporativos   │
                                             │  Brutos)  nizacao)            & Fatos)  │     └─────────────────┘
                                             └─────────────────────────────────────────┘
```


### 2.2.2. Componentes Principais (fontes, EL, transformacao, camadas, visualizacao)


#### Fontes de Dados
- **ERP Sapiens** - Sistema corporativo
- **Sistema AIS** - Sistema industrial

#### Extracao e Carga (EL)
- **Scripts Python** - Extracao dos dados das fontes
- **Apache Airflow** - Orquestracao dos processos

#### Transformacao (T) - Processamento Híbrido

| Tipo | Ferramenta | Frequência | Uso |
|------|------------|------------|-----|
| Batch | dbt Cloud | Varias execucões/dia | Transformacões programadas |
| Near Real-Time (NRT) | Apache Airflow + Scripts Python | Contínuo | Dados críticos |

#### Camadas do Data Warehouse

| Camada | Funcao |
|--------|--------|
| **Raw Layer** | Dados brutos sem transformacao |
| **Source Layer** | Padronizacao de tipos e nomenclatura |
| **Staging Layer** | Limpeza, joins e testes |
| **Marts Layer** | Dimensões e Fatos prontos para consumo |

#### Visualizacao
- **Power BI Service** - Dashboards corporativos para todas as areas

---

---

# 3. Ambiente e Ferramentas

## 3.1. WSL/Linux

### 3.1.1. Navegacao

```bash
pwd                 # Mostra diretorio atual
ls                  # Lista arquivos
ls -la              # Lista com detalhes e ocultos
cd pasta            # Entra na pasta
cd ..               # Volta um nível
cd ~                # Vai para home
```

### 3.1.2. Gerenciamento de Arquivos

```bash
mkdir pasta         # Cria pasta
touch arquivo.txt   # Cria arquivo vazio
cp origem destino   # Copia
mv origem destino   # Move/renomeia
rm arquivo          # Remove arquivo
rm -rf pasta        # Remove pasta e conteúdo
```


### 3.1.3. Gerenciamento de Pacotes

```bash
sudo apt update             # Atualiza lista de pacotes
sudo apt upgrade            # Atualiza pacotes instalados
sudo apt install pacote     # Instala pacote
sudo apt remove pacote      # Remove pacote
```


## 3.2. Python (Ambiente)

### 3.2.1. Criar e Ativar Ambiente Virtual

```bash
python -m venv nome_env     # Cria ambiente
source nome_env/bin/activate    # Ativa (Linux/WSL)
```


### 3.2.2. Gerenciar Dependências

```bash
pip install pacote          # Instala pacote
pip install -r requirements.txt   # Instala do arquivo
pip freeze > requirements.txt     # Exporta dependências
pip list                    # Lista instalados
```

### Desativar
```bash
deactivate                  # Sai do ambiente virtual
```


## 3.3. VS Code

### 3.3.1. Extensões Recomendadas

| Extensao | Uso |
|----------|-----|
| Python | Intellisense e debug |
| Pylance | Autocomplete avancado |
| dbt Power User | Navegacao, preview e lineage dbt |
| GitLens | Historico e blame |
| Remote - WSL | Desenvolvimento no WSL |


### 3.3.2. Atalhos Principais

| Atalho | Acao |
|--------|------|
| `Ctrl + Shift + P` | Command Palette |
| `Ctrl + P` | Buscar arquivo |
| `Ctrl + B` | Toggle sidebar |
| `Ctrl + `` ` | Terminal integrado |
| `Ctrl + Shift + F` | Buscar no projeto |
| `Ctrl + D` | Seleciona proxima ocorrência |
| `Alt + ↑/↓` | Move linha |

---

## 3.4. Git

### 3.4.1. Comandos Basicos

| Comando | Descricao |
|---------|-----------|
| `git init` | Inicializa o repositorio |
| `git status` | Verifica status dos arquivos |
| `git add .` | Adiciona alteracões ao stage |
| `git commit -m` | Cria um commit |
| `git branch` | Lista branches |
| `git checkout -b` | Cria nova branch |
| `git pull` | Atualiza o codigo local |
| `git push` | Envia para o GitHub |


### 3.4.2. Desfazer Alteracões

```bash
git restore arquivo.sql            # Descarta alteracões nao staged
git restore --staged arquivo.sql   # Remove do stage (mantém alteracao)
git reset --soft HEAD~1            # Desfaz último commit (mantém alteracões)
git reset --hard HEAD~1            # Desfaz último commit (perde alteracões)
git clean -fd                      # Remove arquivos nao rastreados
git stash                          # Guarda alteracões temporariamente
git stash pop                      # Recupera alteracões guardadas
git merge --abort                  # Cancela merge em andamento
```


### 3.4.3. Resolucao de Conflitos

```bash
git merge --abort                  # Cancela merge em andamento
```

*Para conflitos detalhados, edite os arquivos manualmente e faca commit apos resolver.*

### 3.4.4. Boas Praticas {#344-boas-praticas}


### 1️⃣ Atualizar branch principal
```bash
git checkout main
git pull
```

### 2️⃣ Criar nova branch
```bash
git checkout -b nome-da-branch
```

**Padrao para branches:** `feature/`, `fix/`, `docs/`

### 3️⃣ Desenvolver e testar
```bash
# Editar arquivos no VS Code
dbt build -s nome_do_modelo        # Executa e testa
```

### 4️⃣ Commitar e enviar
```bash
git status
git add .
git commit -m "feat: cria modelo de vendas"
git push origin -u nome-da-branch
```

### 5️⃣ Abrir Pull Request no GitHub

---

## 3.4.3 Resolucao de Conflitos

### Quando Acontece
Conflitos ocorrem quando duas branches alteram as mesmas linhas.

### Passo a Passo
```bash
git pull origin main               # Atualiza sua branch
git status                         # Identifica arquivos em conflito
```


## 3.5 Fluxo de Desenvolvimento

### 3.5.1. Git → dbt → GitHub (passo a passo)

Abrir arquivo e resolver:
```
<<<<<<< HEAD
seu codigo
=======
codigo da main
>>>>>>> main
```

Remover marcadores, escolher codigo correto, depois:
```bash
git add arquivo_resolvido.sql
git commit -m "fix: resolve conflito"
git push
```

### Cancelar Merge
```bash
git merge --abort                  # Volta ao estado anterior
```

---

## Boas Praticas

- ✅ Commits pequenos e objetivos
- ✅ Mensagens claras: `feat:`, `fix:`, `docs:`
- ✅ Sempre trabalhar com branches
- ✅ `git pull` antes de comecar o dia
- ✅ `dbt build` antes do commit
- ✅ Resolver conflitos imediatamente

---

Fluxo padrao de desenvolvimento local até publicacao no GitHub. Este guia descreve o processo oficial de trabalho com Git e dbt, utilizado para criacao, versionamento e publicacao de modelos no GitHub.

### Fluxo Padrao de Desenvolvimento (Git → dbt → GitHub)

#### 1️⃣ Atualizar a branch principal
```bash
git checkout main
git pull
```

#### 2️⃣ Criar uma nova branch
```bash
git checkout -b nome-da-branch
```

**Padrao recomendado para branches:**
- `feature/nome-do-modelo`
- `fix/correcao-imposto`
- `docs/atualizacao-documentacao`

#### 3️⃣ Realizar as alteracões

Criar ou editar arquivos no VS Code. Exemplos de arquivos: `.sql`, `.yml`, `.md`

#### 4️⃣ Executar o dbt localmente (obrigatorio)

---

#### 6️⃣ Verificar alteracões realizadas
```bash
git status
```

#### 7️⃣ Adicionar arquivos ao stage
```bash
git add .
```

#### 8️⃣ Criar o commit
```bash
git commit -m "mensagem clara e objetiva"
```

**Boas praticas de commit:**
```bash
git commit -m "feat: cria modelo de vendas"
git commit -m "fix: ajusta calculo de ICMS"


```

#### 9️⃣ Enviar a branch para o GitHub
```bash
git push origin -u nome-da-branch
```

Apos a primeira vez:
```bash
git push
```

#### 🔟 Abrir Pull Request no GitHub
1. Acesse o repositorio no GitHub
2. Clique em **Compare & Pull Request**
3. Preencha a descricao
4. Solicite revisao


```bash
# Executar seed normalmente
dbt seed -s nome_do_modelo

# Executar seed com full refresh (substitui todos os dados)
dbt seed -s nome_do_modelo --full-refresh
```

### Fluxo Diario Mais Utilizado

```bash
git status
git add .
git commit -m "mensagem"
git push
```

### Comandos Git Mais Usados

| Comando | Descricao |
|---------|-----------|
| `git init` | Inicializa o repositorio |
| `git status` | Verifica o status dos arquivos |
| `git add .` | Adiciona alteracões ao stage |
| `git commit -m` | Cria um commit |
| `git branch` | Lista branches |
| `git checkout -b` | Cria nova branch |
| `git pull` | Atualiza o codigo local |
| `git push` | Envia para o GitHub |

### Boas Praticas

- ✅ Commits pequenos e objetivos
- ✅ Mensagens claras e padronizadas
- ✅ Sempre trabalhar com branches
- ✅ Executar `git pull` antes de subir alteracões
- ✅ Rodar `dbt run` antes do commit

---

# 4. Stack de Dados

## 4.1 AWS no Contexto Barra Mansa

Visao geral dos servicos AWS utilizados no pipeline de dados.

---

## 4.1. Visao Geral

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│ SQL Server  │ ──── │     S3      │ ──── │  Redshift   │ ──── │  Power BI   │
│   (Fonte)   │      │ (Staging)   │      │    (DW)     │      │   (Consumo) │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
                            │
                     ┌──────┴──────┐
                     │     EC2     │

### 4.2.2. Camadas de Dados {#422-camadas-de-dados}

                     └─────────────┘
```

---

## 4.1.2 S3 (Simple Storage Service)

**O que é:** Armazenamento de arquivos na nuvem (buckets e objetos).

**Uso na Barra Mansa:** area de staging temporaria. Os arquivos CSV/GZIP ficam no S3 entre a extracao (SQL Server) e a carga (Redshift).

**Estrutura:**
```
s3://bm-airflow/
└── comercial/
    ├── tabela1_incremental_temp.csv.gz
    └── tabela2_incremental_temp.csv.gz
```

---

## 4.1.3. EC2 (Elastic Compute Cloud)

**O que é:** Servidores virtuais na AWS.

**Uso na Barra Mansa:** Maquina onde roda o Apache Airflow, executando as DAGs de extracao e carga.

---

## 4.1.4. Redshift

**O que é:** Data Warehouse colunar da AWS, otimizado para consultas analíticas (OLAP).

**Uso na Barra Mansa:** Destino final dos dados. O Power BI conecta aqui para os dashboards.

---

### Distribution Style

Define como os dados sao distribuídos entre os nos do cluster.

| Estilo | Quando usar | Exemplo |
|--------|-------------|---------|
| **KEY** | Tabelas grandes com JOINs frequentes | Fatos (distribui pela FK) |
| **ALL** | Tabelas pequenas usadas em muitos JOINs | Dimensões (replica em todos os nos) |
| **EVEN** | Tabelas sem padrao claro de JOIN | Padrao quando nao especificado |

```sql
CREATE TABLE fato_vendas (
    id_venda INT,
    id_produto INT,
    valor DECIMAL
)
DISTSTYLE KEY
DISTKEY (id_produto);
```

---

### Sort Key

Define a ordem física dos dados no disco. Acelera filtros no `WHERE`.

**Regra pratica:** Use a coluna mais filtrada (geralmente data).

```sql
CREATE TABLE fato_vendas (
    id_venda INT,
    data_venda DATE,
    valor DECIMAL
)
SORTKEY (data_venda);
```

---

### COPY

Comando para carga em massa do S3 para Redshift. Muito mais rapido que INSERT.

```sql
COPY minha_tabela
FROM 's3://bm-airflow/comercial/arquivo.csv.gz'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
DELIMITER ','
GZIP
IGNOREHEADER 1
CSV;
```

---

### EXPLAIN

Mostra o plano de execucao da query antes de rodar. Útil para identificar gargalos.

```sql
EXPLAIN
SELECT * FROM fato_vendas WHERE data_venda > '2025-01-01';
```

**O que observar:**
- `DS_DIST_*` → redistribuicao de dados (lento)
- `Seq Scan` → leitura sequencial (pode ser lento em tabelas grandes)
- Custo alto → query pode demorar

---

### Troubleshooting

#### Ver erros de COPY
```sql
SELECT *
FROM stl_load_errors
ORDER BY starttime DESC
LIMIT 10;
```

#### Ver queries em execucao
```sql
SELECT 
    pid,
    user_name,
    starttime,
    query
FROM stv_recents
WHERE status = 'Running';
```

#### Ver transacões ativas/travadas
```sql
SELECT 
    txn_owner,
    txn_start,
    lock_mode,
    relation
FROM svv_transactions
WHERE lockable_object_type = 'relation';
```

#### Derrubar processo travado
```sql
-- Primeiro: identifique o PID com as queries acima
-- Depois: termine o processo
SELECT pg_terminate_backend(PID_AQUI);
```

⚠️ **Cuidado:** So derrube processos que você tem certeza que estao travados.

---

### Manutencao

```sql
-- Reorganiza dados apos DELETEs (libera espaco)
VACUUM tabela;

-- Atualiza estatísticas para o otimizador
ANALYZE tabela;
```

**Quando rodar:** Apos grandes cargas incrementais ou deletes em massa.

---

# 4.2. dbt (Data Build Tool)

Ferramenta de transformacao de dados que roda SQL no warehouse.

---

## 4.2.1. Estrutura do Projeto

```
dbt_bm/
├── models/
│   ├── staging/
│   │   ├── stg_clientes.sql
│   │   └── stg_vendas.sql
│   └── marts/
│       ├── dim_clientes.sql
│       └── fato_vendas.sql
├── seeds/
│   └── mapeamento_filiais.csv
├── macros/
│   └── grant_permissions.sql
├── tests/
├── dbt_project.yml
└── packages.yml
```

| Pasta | Conteúdo |
|-------|----------|
| `models/` | Arquivos SQL das transformacões |
| `seeds/` | CSVs carregados como tabelas |
| `macros/` | Funcões reutilizaveis (ex: permissões) |
| `tests/` | Testes customizados |

---

## 4.2.2. Camadas de Dados

```
┌─────────┐    ┌─────────┐    ┌─────────────────────┐    ┌─────────┐
│   Raw   │ ── │ Source  │ ── │      Staging        │ ── │  Marts  │
│         │    │         │    │ (staging + intermed)│    │         │
└─────────┘    └─────────┘    └─────────────────────┘    └─────────┘
  Airflow        dbt              dbt                      dbt
  (EL)        (padroniza)     (limpa, join)          (dims e fatos)
```

| Camada | Schema Redshift | Responsavel | Objetivo |
|--------|-----------------|-------------|----------|
| **Raw** | `airbyte_raw` | Airflow | Dados brutos do SQL Server |
| **Source** | `source` | dbt | Padronizacao de nomes e tipos |
| **Staging** | `staging` | dbt | Limpeza, joins, regras de negocio |
| **Marts** | `marts` | dbt | Dimensões e Fatos para consumo |

---

### 4.2.3. Materializacões

Define como o modelo é persistido no banco.

| Tipo | O que faz | Quando usar |
|------|-----------|-------------|
| **table** | Cria tabela física (DROP + CREATE) | Padrao na BM |
| **view** | Cria view (sempre recalcula) | Dados leves, pouca transformacao |
| **incremental** | Insere apenas novos registros | Tabelas grandes com coluna de data |
| **ephemeral** | Nao cria nada (CTE) | Modelo auxiliar usado por outros |

**Configurar no modelo:**
```sql
{{ config(materialized='table') }}

SELECT * FROM {{ ref('stg_clientes') }}
```

**Ou no `dbt_project.yml` (aplica para pasta inteira):**
```yaml
models:
  bm_dbt:
    staging:
      materialized: table
    marts:
      materialized: table
```

---

### 4.2.4. Sources e Refs

#### source() - Referencia tabelas externas (Raw)
```sql
-- models/staging/stg_clientes.sql
SELECT *
FROM {{ source('airbyte_raw', 'e095for') }}
```

#### ref() - Referencia outros modelos dbt
```sql
-- models/marts/dim_clientes.sql
SELECT *
FROM {{ ref('stg_clientes') }}
```

**Por que usar:** dbt monta a ordem de execucao automaticamente (DAG).

---

#### Declarar Sources (schema.yml)

```yaml
# models/staging/schema.yml
version: 2

sources:
  - name: airbyte_raw
    database: producao
    schema: airbyte_raw
    tables:
      - name: e095for
        description: "Cadastro de fornecedores"
      - name: e440nfc
        description: "Notas fiscais de compra"
```

---

### 4.2.5. Testes

Validam qualidade dos dados. Rodam com `dbt test`.

#### Testes Nativos

| Teste | Valida |
|-------|--------|
| `unique` | Valores únicos (sem duplicados) |
| `not_null` | Sem valores nulos |
| `accepted_values` | Valores dentro de lista permitida |
| `relationships` | FK existe na tabela referenciada |

#### Configurar no schema.yml

```yaml
# models/marts/schema.yml
version: 2

models:
  - name: dim_clientes
    columns:
      - name: id_cliente
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['ativo', 'inativo']
```

---

### 4.2.6. Jinja Basico

dbt usa Jinja para SQL dinâmico.

#### Sintaxe

| Sintaxe | Uso |
|---------|-----|
| `{{ }}` | Imprime valor |
| `{% %}` | Logica (if, for) |
| `{# #}` | Comentario |

#### Funcões Comuns

```sql
-- Referências
{{ ref('stg_clientes') }}
{{ source('airbyte_raw', 'e095for') }}

-- Variaveis
{{ var('data_inicio', '2025-01-01') }}

-- Condicional
{% if target.name == 'prod' %}
    schema_producao
{% else %}
    schema_dev
{% endif %}
```

---

### Macro de Permissões (BM)

Macro que concede permissões apos criar tabelas. Roda automaticamente.

```sql
-- macros/grant_permissions.sql
{% macro grant_permissions() %}
    GRANT SELECT ON ALL TABLES IN SCHEMA {{ target.schema }} TO GROUP leitores;
{% endmacro %}
```

**Configurar para rodar apos cada modelo:**
```yaml
# dbt_project.yml
on-run-end:
  - "{{ grant_permissions() }}"
```

---

### Lineage (Grafo de Dependências)

Visualiza a relacao entre modelos.

### Gerar e Visualizar
```bash
dbt docs generate    # Gera documentacao
dbt docs serve       # Abre no navegador
```

### Como Usar
1. Clique no ícone de grafo (canto inferior direito)
2. Busque um modelo
3. Veja dependências (upstream) e dependentes (downstream)

**Útil para:** Entender impacto de mudancas, debugar erros em cascata.

---

### 4.2.7. Comandos dbt


#### Execucao
```bash
dbt run                            # Executa todos os modelos
dbt run -s nome_do_modelo          # Executa modelo específico
dbt run -s +nome_do_modelo         # Modelo + filhos (downstream)
dbt run -s @nome_do_modelo         # Modelo + pais e filhos
```

#### Testes e Build
```bash
dbt test                           # Executa todos os testes
dbt test -s nome_do_modelo         # Testa modelo específico
dbt build                          # Executa run + test
dbt build -s nome_do_modelo        # Build de modelo específico
```

#### Seeds e Dependências
```bash
dbt seed                           # Carrega todos os seeds
dbt seed -s nome_do_seed           # Carrega seed específico
dbt seed --full-refresh            # Substitui todos os dados
dbt deps                           # Instala packages do packages.yml
```

#### Debug e Documentacao
```bash
dbt debug                          # Verifica conexao e configuracao
dbt compile                        # Compila SQL sem executar
dbt compile -s nome_do_modelo      # Compila modelo específico
dbt docs generate                  # Gera documentacao
dbt docs serve                     # Abre documentacao no navegador
```

---

## 4.3. Airflow

Orquestrador de pipelines de dados.

### 4.3.1. Conceitos Essenciais


**Airflow** agenda, executa e monitora pipelines de dados.

**Por que usar (vs cron/scripts):**

| Cron/Scripts | Airflow |
|--------------|---------|
| Sem visualizacao | Interface web com fluxo visual |
| Dependências manuais | Dependências automaticas |
| Logs espalhados | Logs centralizados |
| Retry manual | Retry automatico |

---


```
┌───────┐      ┌───────┐      ┌───────┐
│ Task  │ ───► │ Task  │ ───► │ Task  │
│   A   │      │   B   │      │   C   │
└───────┘      └───────┘      └───────┘
 extrair       transformar     carregar
```

| Conceito | O que é |
|----------|---------|
| **DAG** | Fluxo de tarefas (grafo sem ciclos) |
| **Task** | Unidade de trabalho |
| **Dependência** | Ordem de execucao |

---


### 4.3.2. Operadores {#432-operadores}


Operador define **o que** a task faz.

| Operador | O que faz |
|----------|-----------|
| `@task` | Executa funcao Python (padrao) |
| `BranchPythonOperator` | Decide qual caminho seguir |
| `EmptyOperator` | Placeholder |
| `TriggerDagRunOperator` | Dispara outra DAG |

**`@task`** é um atalho para `PythonOperator`:

```python
# Com decorator (recomendado)
@task
def extrair():
    return dados

# Equivalente
PythonOperator(task_id="extrair", python_callable=extrair_func)
```

---

### 4.3.3. Anatomia de uma DAG

```python
from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="minha_dag",                        # Nome único
    start_date=pendulum.datetime(2025, 1, 1),  # Data inicial
    schedule_interval="0 8 * * *",             # Quando roda (cron)
    catchup=False,                             # Nao executa passadas
    tags=["exemplo"],                          # Categorizacao
) as dag:

    @task
    def extrair():
        return "dados"

    @task
    def carregar(dados):
        print(dados)

    # Dependência: extrair → carregar
    resultado = extrair()
    carregar(resultado)
```

---

### 4.3.4. Hooks e Conexões

**Hook** = conector para sistemas externos.

```python
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# SQL Server
hook = MsSqlHook(mssql_conn_id="mssql_bm_conn")
cursor = hook.get_conn().cursor()
cursor.execute("SELECT * FROM tabela")

# Redshift
hook = PostgresHook(postgres_conn_id="redshift_conn")
hook.run("TRUNCATE TABLE destino")
```

---

### 4.3.5. Recursos Intermediarios

####  Context e Params

**Context:** Informacões da execucao.

```python
@task
def minha_task(**context):
    data = context["ds"]           # Data execucao
    params = context["params"]     # Parâmetros passados
```

**Params:** Parâmetros via UI/CLI.

```python
with DAG(
    params={"custom_tables": Param(default=[], type=["null", "array"])}
) as dag:
    ...

# Disparar: {"custom_tables": ["e640lct"]}
```

---

#### Controle de Fluxo

**TriggerRule:** Quando a task executa.

| Regra | Executa quando |
|-------|----------------|
| `ALL_SUCCESS` | Todas anteriores OK (padrao) |
| `ALL_DONE` | Todas finalizadas (sucesso ou falha) |

**AirflowSkipException:** Pula task sem falhar.

```python
from airflow.exceptions import AirflowSkipException

@task
def processar(tabela, **context):
    permitidas = context["params"].get("custom_tables", [])
    if permitidas and tabela not in permitidas:
        raise AirflowSkipException(f"{tabela} nao esta na lista")
```

---

#### TaskGroups

Agrupa tasks visualmente.

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup(group_id="vendas") as grupo:
    extrair_clientes()
    extrair_produtos()
```

---

#### Modularizacao

**O que:** Separar DAG (orquestracao) de logica (funcões).

**Por que:** Reutilizacao, testes, manutencao.

**Como:**
```
dags/                    # Orquestracao
scripts/python/          # Funcões reutilizaveis
include/seed/            # Configuracões
```

```python
# DAG importa funcões do modulo
from scripts.python.get_el_tasks import extract_upload

@task
def extrair(tabela):
    return extract_upload(tabela, S3_BUCKET)
```

---

#### Geracao Dinâmica

Cria tasks automaticamente a partir de lista ou arquivo.

##### Via Lista

```python
TABELAS = ["e095for", "e440nfc", "e660inv"]

for tabela in TABELAS:
    @task(task_id=f"process_{tabela}")
    def processar(t=tabela):
        extract_upload(t)
    processar()
```

**Resultado:** 3 tasks criadas automaticamente.

#### Via CSV

```python
import pandas as pd

df = pd.read_csv("nrt_dependencies.csv", sep=";")

for _, row in df.iterrows():
    criar_task(row["tabela_origem"], row["tabela_fato"])
```

**Vantagem:** Adicionar tabela = nova linha (sem mexer em codigo).

---


### 4.3.6. Airflow na Barra Mansa


#### Visao Geral

**Números:** 70+ DAGs | 300+ tabelas | ~7k execucões/dia

**Fluxo EL:**
```
SQL Server  ──►  S3  ──►  Redshift
 (fonte)      (staging)    (DW)
```

**Conexões:**

| Connection ID | Sistema |
|---------------|---------|
| `mssql_bm_conn` | SQL Server Sapiens |
| `mssql_bm_vetorh` | SQL Server VetorH |
| `mssql_bm_ais` | SQL Server AIS |
| `redshift_conn` | Redshift DW |

---

### Estratégias de Carga

| Estratégia | Operacao | Uso | Schedule |
|------------|----------|-----|----------|
| **full_auto** | DROP → CREATE → COPY | Cadastros pequenos | Diario |
| **full_manual** | DROP → CREATE → COPY | Tabelas grandes | Manual |
| **incremental** | DELETE (120 dias) → COPY | Transacões | Diario |
| **nrt** | DELETE (60 dias) → COPY | Dados críticos | 5min |

**Por que full_auto vs full_manual?**
- Tabelas grandes travam se rodam junto
- Manual permite controle

**Janela incremental:**
- DELETE apaga últimos N dias
- COPY insere dados atualizados
- Evita duplicacao


### 4.3.7. Como Adicionar Nova Tabela


### DAGs Principais

#### job_sapiens_full_auto

Carga diaria de cadastros (48 tabelas pequenas).

```
Lista → Para cada: DROP → CREATE → COPY
```

---

#### job_sapiens_full_manual

Carga manual de tabelas grandes (43 tabelas).

```
Lista → Para cada: DROP → CREATE → COPY
```

Usado para carga inicial antes de ativar incremental.

---

#### job_sapiens_incremental

Carga incremental de transacões (43 tabelas).

```
┌─────────────────────────────────────────────┐
│ job_sapiens_incremental                     │
├─────────────────────────────────────────────┤
│  Lista (tables.py)                          │
│       │                                     │
│       ▼                                     │
│  ┌────────────┐  ┌────────────┐            │
│  │ dom_vendas │  │ dom_fiscal │  ...       │
│  │ ├─ e140nfv │  │ ├─ e660inv │            │
│  │ └─ e120ped │  │ └─ e440nfc │            │
│  └────────────┘  └────────────┘            │
│                                             │
│  Cada task: DELETE(120d) → COPY            │
└─────────────────────────────────────────────┘
```

---

#### job_controladoria_financeira_nrt

Pipeline NRT: ingestao + transformacao a cada 5min (60+ tabelas).

```
┌─────────────────────────────────────────────────────┐
│ job_controladoria_financeira_nrt                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  CSV (nrt_dependencies.csv)                         │
│       │                                             │
│       ▼                                             │
│  ┌──────────────┐      ┌───────────────┐           │
│  │ ingestion    │      │ transformation│           │
│  │ ├─ e095for ──┼─────►│ ├─ dim_fornec │           │
│  │ └─ e440nfc ──┼─────►│ └─ fato_compra│           │
│  └──────────────┘      └───────┬───────┘           │
│                                │                    │
│                                ▼                    │
│                        ┌─────────────┐             │
│                        │ log_execucao│             │
│                        └──────┬──────┘             │
│                               │                     │
│                               ▼                     │
│                        ┌─────────────┐             │
│                        │trigger_valid│──► obs_dag  │
│                        └─────────────┘             │
└─────────────────────────────────────────────────────┘
```

**Arquivos de configuracao:**

| Arquivo | Conteúdo |
|---------|----------|
| `nrt_dependencies.csv` | origem → fato |
| `nrt_dominios_origem.csv` | domínio origem |
| `nrt_dominios.csv` | domínio fato |

---

### Como Adicionar Nova Tabela

#### Cenario 1: Tabela Full (cadastro pequeno)

| Passo | Arquivo | Acao |
|-------|---------|------|
| 1 | `schema_sapiens.csv` | Adicionar colunas |
| 2 | `tables.py` | Adicionar em `TABELAS_FULL_AUTO` |

Proxima execucao processa automaticamente.

---

#### Cenario 2: Tabela Incremental (transacional)

| Passo | Arquivo | Acao |
|-------|---------|------|
| 1 | `schema_sapiens.csv` | Adicionar colunas |
| 2 | `incremental_config.csv` | Adicionar config |
| 3 | `tables.py` | Adicionar em `TABELAS_INCREMENTAL` |

```bash
# Carga inicial (obrigatorio)
airflow dags trigger job_sapiens_full_manual \
    --conf '{"custom_tables": ["nova_tabela"]}'
```

Proximas execucões: incremental automatico.

---

### 4.3.8. Operacao

#### Cenario 3: Tabela NRT (transacional + transformacao)

| Passo | Arquivo | Acao |
|-------|---------|------|
| 1-3 | (Cenario 2) | Config raw incremental |
| 4 | `nrt_dependencies.csv` | Mapear origem → fato |
| 5 | `nrt_dominios_origem.csv` | Domínio origem |
| 6 | `nrt_dominios.csv` | Domínio fato |
| 7 | `models/marts/` | Criar model dbt |

```bash
# Carga inicial raw
airflow dags trigger job_sapiens_full_manual \
    --conf '{"custom_tables": ["nova_tabela"]}'

# Testar transformacao
dbt run -s fato_nova
```

---

#### Resumo

| Tipo | Cenario | DAG |
|------|---------|-----|
| Cadastro pequeno | 1 | job_sapiens_full_auto |
| Transacional | 2 | job_sapiens_incremental |
| Transacional + dashboard | 3 | job_controladoria_nrt |

---

### Operacao

#### Rodar Tabela Específica

**UI:** Trigger DAG w/ config → `{"custom_tables": ["e640lct"]}`

**CLI:**
```bash
airflow dags trigger job_sapiens_incremental \
    --conf '{"custom_tables": ["e640lct"]}'
```

#### Monitoramento

| Status | Cor | Acao |
|--------|-----|------|
| Success | 🟢 | OK |
| Failed | 🔴 | Ver logs → Clear |
| Skipped | 🟣 | Normal |
| Running | 🟡 | Aguardar |

#### Troubleshooting


## 4.4. Power BI

#### Ferramenta de visualizacao e analise de dados.

**Fluxo de trabalho:**
```
Conectar  ──►  Modelar  ──►  Visualizar  ──►  Publicar
 (fontes)     (relacionar)    (graficos)      (compartilhar)
```

**Componentes:**
| Componente | Uso |
|------------|-----|
| Power Query | Conexao e transformacao de dados |
| Modelo | Relacionamentos entre tabelas |
| Relatorio | Criacao de visuais |
| Servico | Publicacao e compartilhamento |

---

### 4.4.1. Conexao com Fontes

#### Redshift (conector nativo)
1. Obter Dados → Banco de Dados → Amazon Redshift
2. Informar servidor e banco
3. Credenciais → Banco de Dados
4. Selecionar tabelas

#### Excel e CSV
1. Obter Dados → Arquivo → Excel / CSV
2. Selecionar arquivo
3. Escolher planilha/tabela

### 4.4.2. Modelagem de Dados

#### Google Sheets
1. Obter Dados → Mais → Google Sheets
2. Autenticar conta Google
3. Selecionar planilha

#### Import vs DirectQuery

| Modo | Dados | Quando usar |
|------|-------|-------------|
| **Import** | Carregados no Power BI | Analises complexas, melhor performance |
| **DirectQuery** | Consultados em tempo real | Dados muito grandes, sempre atualizados |

**Padrao BM:** Import (atualizacao agendada).

---

### Modelagem de Dados

#### Star Schema

Modelo ideal para analise: tabela **Fato** no centro, **Dimensões** ao redor.

```
                    ┌─────────────┐
                    │ dim_produto │
                    └──────┬──────┘
                           │
┌─────────────┐     ┌──────┴──────┐     ┌─────────────┐
│ dim_cliente │─────│ fato_vendas │─────│  dim_data   │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ dim_vendedor│
                    └─────────────┘
```

| Tipo | Conteúdo | Exemplo |
|------|----------|---------|
| **Fato** | Métricas, transacões | vendas, saídas, lancamentos |
| **Dimensao** | Atributos descritivos | produto, cliente, data |


### 4.4.3. DAX Intermediario


**Criar:** Arrastar campo de uma tabela para outra.

**Cardinalidade:**
| Tipo | Significado |
|------|-------------|
| 1:N | Um registro da dimensao para N da fato (padrao) |
| N:N | Evitar quando possível |

#### Tabelas De-Para (Lookup)

Traduz codigos para descricões ou agrupa categorias.

**Exemplo:** `depara_categoria`
| cod_produto | categoria |
|-------------|-----------|
| 001-100 | Carnes |
| 101-200 | Frios |
| 201-300 | Embutidos |

---

### DAX Intermediario

#### Colunas Calculadas vs Medidas

| Tipo | Calculado | Armazenado | Quando usar |
|------|-----------|------------|-------------|
| **Coluna** | Ao atualizar | Sim (ocupa espaco) | Classificacões fixas, lookups |
| **Medida** | Ao exibir | Nao | Agregacões, KPIs |

**Regra:** Prefira medidas sempre que possível.

---

#### Funcões Basicas

```dax
// Agregacões
Total Vendas = SUM(fato_vendas[valor])
Media Vendas = AVERAGE(fato_vendas[valor])
Qtd Registros = COUNT(fato_vendas[id])
Qtd Clientes = DISTINCTCOUNT(fato_vendas[id_cliente])
```

---

#### CALCULATE (Mudar Contexto)

Aplica filtros a agregacao.

```dax
// Vendas apenas de 2025
Vendas 2025 = 
CALCULATE(
    SUM(fato_vendas[valor]),
    dim_data[ano] = 2025
)

// Vendas da categoria "Carnes"
Vendas Carnes = 
CALCULATE(
    SUM(fato_vendas[valor]),
    dim_produto[categoria] = "Carnes"
)
```

---

#### RELATED (Buscar de Outra Tabela)

Traz valor de tabela relacionada (dimensao → fato).

```dax
// Coluna calculada na fato_vendas
Categoria = RELATED(dim_produto[categoria])
```

---

#### SUMMARIZE (Criar Tabela Agregada)

```dax
// Tabela com total por categoria

### 4.4.4. Design de Layouts {#444-design-de-layouts}

SUMMARIZE(
    fato_vendas,
    dim_produto[categoria],
    "Total", SUM(fato_vendas[valor]),
    "Qtd", COUNT(fato_vendas[id])
)
```

---

#### Tabelas Calculadas

```dax
// Tabela de datas (calendario)
Calendario = 
CALENDAR(DATE(2020,1,1), DATE(2030,12,31))

// Tabela de parâmetros
Metas = 
DATATABLE(
    "Mes", INTEGER,
    "Meta", CURRENCY,
    {
        {1, 100000},
        {2, 120000},
        {3, 110000}
    }
)
```

---

#### Exemplo: % Realizado vs Orcado

### 4.4.5. Criacao de Visuais

```dax
// Medidas
Realizado = SUM(fato_lancamentos[valor])

Orcado = SUM(dim_orcamento[valor])

% Variacao = 
DIVIDE(
    [Realizado] - [Orcado],
    [Orcado],
    0
)
```

---

### Design de Layouts

#### Figma para Tela de Fundo

**Por que:** Controle total sobre design, consistência visual.

**Como:**
1. Criar frame 1920x1080 (ou 1280x720)
2. Definir areas: cabecalho, filtros, graficos
3. Aplicar cores e formas
4. Exportar como PNG

**Aplicar no Power BI:**
1. Formato → Tela de fundo da pagina
2. Imagem → Selecionar arquivo
3. Ajustar transparência se necessario

### 4.4.6. Interatividade

#### Princípios de Design

| Princípio | Aplicacao |
|-----------|-----------|
| **Hierarquia** | KPIs principais no topo |
| **Alinhamento** | Visuais alinhados em grade |
| **Proximidade** | Agrupar informacões relacionadas |
| **Contraste** | Destaque para números importantes |

---

### Criacao de Visuais

#### Quando Usar Cada Visual

| Visual | Uso |
|--------|-----|
| **Cartao** | KPI único (total, média) |
| **Tabela** | Dados detalhados |
| **Matriz** | Dados com hierarquia (drill-down) |
| **Barra** | Comparar categorias |
| **Linha** | Evolucao no tempo |
| **Combo** | Duas métricas com escalas diferentes |

#### Tabelas e Matrizes

**Tabela:** Lista simples de registros.

**Matriz:** Linhas e colunas com agregacao.
- Permite drill-down (expandir/recolher)

### 4.4.7. Boas Praticas {#447-boas-praticas}


---

### Interatividade

#### Segmentacao (Slicers)

Filtros visuais para o usuario.

**Tipos:**
- Lista (selecao única/múltipla)
- Dropdown (economiza espaco)
- Entre (range de datas/valores)

#### Filtros

| Nível | Afeta |
|-------|-------|
| Visual | Apenas o visual selecionado |
| Pagina | Todos os visuais da pagina |
| Relatorio | Todas as paginas |

#### Drill-down

Navegar em hierarquias (ex: Ano → Mês → Dia).

1. Criar hierarquia na dimensao
2. Adicionar ao visual
3. Usar ícones de drill no visual

---

### Boas Praticas

#### Layouts

- ✅ Fundo neutro (cinza claro, branco)

### 4.4.8. Exemplos Contextualizados {#448-exemplos-contextualizados}

- ✅ KPIs no topo
- ✅ Filtros a esquerda ou topo
- ❌ Cores vibrantes em excesso
- ❌ Graficos 3D

#### Tabelas Auxiliares

| Tabela | Uso |
|--------|-----|
| Calendario | Analises temporais, time intelligence |
| De-Para | Categorizacao, traducao de codigos |
| Parâmetros | Metas, configuracões |

#### Tipagem de Colunas

| Tipo | Usar para |
|------|-----------|
| Texto | Codigos, descricões |
| Número inteiro | IDs, quantidades |
| Número decimal | Valores monetarios |
| Data | Datas (nao texto!) |

#### Nomenclatura

- Tabelas: `fato_`, `dim_`, `depara_`
- Medidas: Comecar com verbo (Total, Qtd, %)
- Colunas: snake_case ou PascalCase (consistente)

#### Performance

- ✅ Usar medidas (nao colunas calculadas)
- ✅ Remover colunas nao usadas
- ✅ Evitar relacionamentos N:N
- ❌ Colunas calculadas com RELATED em tabelas grandes

---

## Exemplos Contextualizados

### Dashboard: Saídas de Estoque

**Modelo:**
```
┌─────────────────┐     ┌─────────────────┐
│ depara_categoria│     │   dim_produto   │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
              ┌──────┴──────┐
              │ fato_saidas │
              └──────┬──────┘
                     │
              ┌──────┴──────┐
              │  dim_data   │
              └─────────────┘
```

**Medidas:**
```dax
Total Saidas = SUM(fato_saidas[quantidade])

Saidas por Categoria = 
CALCULATE(
    [Total Saidas],
    USERELATIONSHIP(dim_produto[cod_categoria], depara_categoria[cod_categoria])
)

Ranking Categoria = 
RANKX(
    ALL(depara_categoria[categoria]),
    [Total Saidas]
)
```

**Visuais:**
- Cartões: Total saídas, Qtd categorias
- Matriz: Categoria → Produto (com drill-down)
- Grafico barra: Top 10 categorias
- Slicer: Período

---

### Dashboard: Acompanhamento Orcamentario

**Modelo:**
```
┌─────────────────┐     ┌─────────────────┐
│  depara_classe  │     │   dim_conta     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
            ┌────────┴────────┐
            │ fato_lancamentos│
            └────────┬────────┘
                     │
         ┌───────────┼───────────┐
         │           │           │
  ┌──────┴───┐ ┌─────┴─────┐ ┌───┴────┐
  │ dim_data │ │dim_depto  │ │orcamento│
  └──────────┘ └───────────┘ └─────────┘
```

**Medidas:**
```dax
Realizado = SUM(fato_lancamentos[valor])

Orcado = SUM(orcamento[valor])

Variacao = [Realizado] - [Orcado]

% Variacao = 
DIVIDE([Variacao], [Orcado], 0)

Status = 
IF(
    [% Variacao] > 0.1, "Acima",
    IF([% Variacao] < -0.1, "Abaixo", "OK")
)
```

**Visuais:**
- Cartões: Realizado, Orcado, % Variacao
- Tabela: Classe | Realizado | Orcado | Variacao | Status
- Grafico linha: Tendência mensal (Realizado vs Orcado)
- Slicers: Departamento, Período, Classe

---

### Ex: Power BI - Analise de Vendas (NF-e Saída 2020-2025)

Sugestao de analise utilizando a base historica de notas fiscais de saída.

### Objetivo

Acompanhar a evolucao do faturamento ao longo do tempo e identificar padrões de sazonalidade que impactam o negocio.

### O que deve ser construído no Power BI

1. **Faturamento mensal e anual** - Grafico de linhas mostrando evolucao temporal
2. **Comparativo ano a ano** - Faturamento por ano para identificar crescimento/queda
3. **Analise de sazonalidade** - Identificar meses de pico e baixa nas vendas
4. **Top 10 produtos** - Quais produtos mais faturam (curva ABC simples)
5. **Volume de notas emitidas** - Quantidade de NF-e por mês (indicador operacional)

### Visualizacões Sugeridas

- Grafico de linha para evolucao mensal
- Grafico de barras para comparativo anual
- Tabela com top produtos por faturamento

### Valor da Analise

Visao rapida e clara do desempenho comercial, identificacao de tendências e padrões sazonais para planejamento.

| Complexidade | Execucao | Valor Informacional |
|--------------|----------|---------------------|
| Baixa | Rapida | Alto |

---


---

# 5. Qualidade e Validacao

## 5.1. Validacao Cruzada

### 5.1.1. Importância {#511-importância}

A validacao cruzada é essencial para garantir a integridade e confiabilidade dos dados transformados.

### 5.1.2. Princípios {#512-princípios}

- Comparar totais entre origem e destino
- Validar cardinalidades e relacionamentos
- Verificar consistência de agregacões

### 5.1.3. Recomendacao {#513-recomendacao}

Realizar validacao tripla: Origem (SQL Server) → Transformacao (dbt) → Destino (Redshift/Power BI)


---

# 6. Pratica

## 6.1. Exercícios SQL Analíticos


Exercícios propostos para desenvolver habilidade essencial de Analise de Dados: realizar consultas SQL analíticas que permitam responder perguntas de negocio de forma eficiente e precisa.

### Estrutura dos Exercícios

| Bloco | Exercícios | Conceito | Nível |
|-------|------------|----------|-------|
| 1 | EX1-5 | SELECT + WHERE | ⭐ |
| 2 | EX6-10 | JOINs | ⭐⭐ |
| 3 | EX11-15 | Agregacões | ⭐⭐ |
| 4 | EX16-20 | CTEs | ⭐⭐⭐ |
| 5 | EX21-25 | Window Functions | ⭐⭐⭐ |
| 6 | EX26-30 | UNION + Subconsultas + DML | ⭐⭐⭐ |

### Tabelas Principais

`E440NFC`, `E440IPC`, `E095FOR`, `E440ISC`, `E660INC`, `E075PRO`, `E070FIL`

### Aplicacões

Contabil | Fiscal | Compras | Financeiro | Controladoria

---

### Bloco 1: Basico (SELECT + WHERE) ⭐

#### EX1: Consulta de Lancamentos Contabeis com Filtro por Conta Específica

**Objetivo:** Extrair lancamentos contabeis da tabela E640LCT

**Retornar:** Codigo Empresa, Codigo Filial, Número Lancamento, Data Lancamento, Conta Débito, Conta Crédito, Valor Lancamento e Complemento de Lancamento.

**Critérios:** Ano de 2025 e lancamentos que envolvam a Conta 11730.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    CodEmp AS empresa,
    CodFil AS filial,
    NumLct AS numero_lancamento,
    DatLct AS data_lancamento,
    CtaDeb AS conta_debito,
    CtaCre AS conta_credito,
    VlrLct AS valor_lancamento,
    CplLct AS complemento
FROM E640LCT
WHERE DatLct BETWEEN '20250101' AND '20251231'
    AND (CtaDeb = 11730 OR CtaCre = 11730)
```

</details>

---

#### EX2: Analise de Itens de Produto em Notas Fiscais de Entrada

**Objetivo:** Consultar itens de produto da tabela E440IPC (Compras - Itens de Produto)

**Retornar:** Codigo Empresa, Codigo Filial, Número da NF, Codigo do Fornecedor, Codigo do Produto, Quantidade Recebida, Preco Unitario, Valor Líquido e Data de Geracao.

**Critérios:** Primeiro semestre de 2025 (Janeiro a Junho) com quantidade superior a 10 unidades.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    CodEmp AS empresa,
    CodFil AS filial,
    NumNfc AS numero_nota,
    CodFor AS codigo_fornecedor,
    CodPro AS codigo_produto,
    QtdRec AS quantidade_recebida,
    PreUni AS preco_unitario,
    VlrLiq AS valor_liquido,
    DatGer AS data_geracao
FROM E440IPC
WHERE DatGer BETWEEN '20250101' AND '20250630'
    AND QtdRec > 10
```

</details>

---

#### EX3: Relatorio de Servicos com Impostos Retidos

**Objetivo:** Extrair itens de servico da tabela E440ISC (Compras - Itens de Servico)

**Retornar:** Codigo Empresa, Codigo Filial, Número da NF, Codigo do Fornecedor, Codigo do Servico, Valor Bruto, Valor de ISS, Valor de IRRF e Data de Geracao.

**Critérios:** Mês de Marco de 2025, ordenado por valor bruto decrescente.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    CodEmp AS empresa,
    CodFil AS filial,
    NumNfc AS numero_nota,
    CodFor AS codigo_fornecedor,
    CodSer AS codigo_servico,
    VlrBru AS valor_bruto,
    VlrIss AS valor_iss,
    VlrIrf AS valor_irrf,
    DatGer AS data_geracao
FROM E440ISC
WHERE DatGer BETWEEN '20250301' AND '20250331'
ORDER BY VlrBru DESC
```

</details>

---

#### EX4: Levantamento de Notas Fiscais Fechadas com Tributos

**Objetivo:** Consultar dados gerais de notas fiscais na tabela E440NFC (Compras - Dados Gerais)

**Retornar:** Codigo Empresa, Codigo Filial, Número da NF, Codigo do Fornecedor, Data de Emissao, Data de Entrada, Valor de Produtos, Valor de ICMS, Valor de IPI e Situacao da NF.

**Critérios:** Segundo trimestre de 2025 (Abril a Junho) e status 'F' (Fechada).

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    CodEmp AS empresa,
    CodFil AS filial,
    NumNfc AS numero_nota,
    CodFor AS codigo_fornecedor,
    DatEmi AS data_emissao,
    DatEnt AS data_entrada,
    VlrBpr AS valor_produtos,
    VlrIcm AS valor_icms,
    VlrIpi AS valor_ipi,
    SitNfc AS situacao_nota
FROM E440NFC
WHERE DatEnt BETWEEN '20250401' AND '20250630'
    AND SitNfc = 'F'
```

</details>

---

#### EX5: Apuracao Fiscal de Itens com Valor Relevante

**Objetivo:** Analisar itens fiscais da tabela E660INC (Impostos - Itens de Produto/Servico)

**Retornar:** Codigo Filial, Codigo do Fornecedor, Número da NF, Codigo do Produto, Quantidade de Entrada, Valor Contabil, Valor de ICMS, Percentual de ICMS e Data de Geracao.

**Critérios:** Primeira quinzena de Janeiro de 2025 (01/01 a 15/01) com valor contabil superior a R$ 500,00.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    CodFil AS filial,
    CodFor AS codigo_fornecedor,
    NumNfi AS numero_nota,
    CodPro AS codigo_produto,
    QtdEnt AS quantidade_entrada,
    VlrCtb AS valor_contabil,
    VlrIcm AS valor_icms,
    PerIcm AS percentual_icms,
    DatGer AS data_geracao
FROM E660INC
WHERE DatGer BETWEEN '20250101' AND '20250115'
    AND VlrCtb > 500
```

</details>

---

### Bloco 2: JOINs ⭐⭐

#### EX6: Analise Completa de Compras com Dados de Fornecedores

**Objetivo:** Consultar notas fiscais de entrada relacionando com dados cadastrais de fornecedores

**Retornar:** Codigo Empresa, Codigo Filial, Número NF, Nome do Fornecedor, CNPJ, Cidade, Data de Emissao, Valor Total, Status da NF.

**Critérios:** Primeiro trimestre de 2025 (Janeiro a Marco), ordenado por valor decrescente.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    nfc.CodEmp AS codigo_empresa,
    nfc.CodFil AS codigo_filial,
    nfc.NumNfc AS numero_nf,
    fornec.NomFor AS nome_fornecedor,
    fornec.CgcCpf AS cnpj_fornecedor,
    fornec.CidFor AS cidade_fornecedor,
    nfc.DatEmi AS data_emissao,
    nfc.VlrLiq AS valor_total,
    nfc.SitNfc AS status_nf
FROM E440NFC AS nfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
WHERE nfc.DatEmi BETWEEN '20250101' AND '20250331'
ORDER BY nfc.VlrLiq DESC
```

</details>

---

#### EX7: Itens de Produto com Informacões Detalhadas de Produto e Fornecedor

**Objetivo:** Analisar itens de compra relacionando produtos, fornecedores e notas fiscais

**Retornar:** Número NF, Nome Fornecedor, Codigo Produto, Descricao Produto, Quantidade Recebida, Preco Unitario, Valor Total do Item, Data de Entrada.

**Critérios:** Mês de Fevereiro 2025, produtos com valor total superior a R$ 1.000,00.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    ipc.NumNfc AS numero_nf,
    fornec.NomFor AS nome_fornecedor,
    ipc.CodPro AS codigo_produto,
    prod.DesPro AS descricao_produto,
    ipc.QtdRec AS quantidade_recebida,
    ipc.PreUni AS preco_unitario,
    (ipc.QtdRec * ipc.PreUni) AS valor_total_item,
    nfc.DatEnt AS data_entrada
FROM E440IPC AS ipc
    INNER JOIN E440NFC AS nfc
        ON ipc.CodEmp = nfc.CodEmp
        AND ipc.CodFil = nfc.CodFil
        AND ipc.NumNfc = nfc.NumNfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
    INNER JOIN E075PRO AS prod
        ON ipc.CodPro = prod.CodPro
WHERE nfc.DatGer BETWEEN '20250201' AND '20250228'
    AND (ipc.QtdRec * ipc.PreUni) > 1000
ORDER BY valor_total_item DESC
```

</details>

---

#### EX8: Consolidacao de Impostos com Filiais e Fornecedores

**Objetivo:** Relacionar dados fiscais de entrada com informacões organizacionais

**Retornar:** Nome Filial, Nome Fornecedor, Número NF, Data Emissao, Valor Base ICMS, Valor ICMS, Valor IPI, Valor Total Tributos.

**Critérios:** Segundo trimestre de 2025 (Abril a Junho), agrupado por filial.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    fil.NomFil AS nome_filial,
    fornec.NomFor AS nome_fornecedor,
    nfc.NumNfi AS numero_nf,
    nfc.DatEmi AS data_emissao,
    inc.VlrBic AS base_icms,
    inc.VlrIcm AS valor_icms,
    inc.VlrIpi AS valor_ipi,
    (ISNULL(inc.VlrIcm, 0) + ISNULL(inc.VlrIpi, 0) +
     ISNULL(inc.VlrPir, 0) + ISNULL(inc.VlrCor, 0)) AS total_tributos
FROM E660INC AS inc
    INNER JOIN E660NFC AS nfc
        ON inc.CodFil = nfc.CodFil
        AND inc.CodFor = nfc.CodFor
        AND inc.NumNfi = nfc.NumNfi
    INNER JOIN E070FIL AS fil
        ON nfc.CodFil = fil.CodFil
        AND nfc.CodEmp = fil.CodEmp
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
WHERE nfc.DatGer BETWEEN '20250401' AND '20250630'
ORDER BY fil.NomFil, total_tributos DESC
```

</details>

---

#### EX9: Analise de Servicos com Centro de Custo e Fornecedor

**Objetivo:** Relacionar servicos contratados com centros de custo e fornecedores

**Retornar:** Nome Fornecedor, Descricao Servico, Centro de Custo, Descricao Centro Custo, Quantidade, Valor Unitario, Valor Total, Valor ISS Retido, Data Geracao.

**Critérios:** Janeiro a Marco 2025, apenas servicos com ISS retido, ordenado por centro de custo.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    fornec.NomFor AS nome_fornecedor,
    serv.DesSer AS descricao_servico,
    isc.CodCcu AS centro_custo,
    ccu.DesCcu AS descricao_centro_custo,
    isc.QtdRec AS quantidade,
    isc.PreUni AS valor_unitario,
    isc.VlrLiq AS valor_liquido,
    isc.VlrIss AS valor_iss,
    nfc.DatGer AS data_geracao
FROM E440ISC AS isc
    INNER JOIN E440NFC AS nfc
        ON isc.CodEmp = nfc.CodEmp
        AND isc.CodFil = nfc.CodFil
        AND isc.NumNfc = nfc.NumNfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
    LEFT JOIN E080SER AS serv
        ON isc.CodSer = serv.CodSer
    LEFT JOIN E044CCU AS ccu
        ON isc.CodCcu = ccu.CodCcu
        AND isc.CodEmp = ccu.CodEmp
WHERE nfc.DatGer BETWEEN '20250101' AND '20250331'
    AND isc.VlrIss > 0
ORDER BY ccu.DesCcu, isc.VlrLiq DESC
```

</details>

---

#### EX10: Rastreamento Completo: Compra, Contabilizacao e Impostos

**Objetivo:** Relacionar compras com lancamentos contabeis e apuracao fiscal em uma visao integrada

**Retornar:** Número NF, Nome Fornecedor, Descricao Produto, Conta Contabil Débito, Descricao Conta, Valor Contabil, Base ICMS, Valor ICMS, Data Lancamento.

**Critérios:** Fevereiro 2025, apenas itens com lancamento contabil e ICMS destacado.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT DISTINCT
    ipc.NumNfc AS numero_nf,
    fornec.NomFor AS nome_fornecedor,
    prod.DesPro AS descricao_produto,
    lct.CtaDeb AS conta_debito,
    pla.DesCta AS descricao_conta,
    inc.VlrCtb AS valor_contabil,
    inc.VlrBic AS base_icms,
    inc.VlrIcm AS valor_icms,
    lct.DatLct AS data_lancamento
FROM E440IPC AS ipc
    INNER JOIN E440NFC AS nfc
        ON ipc.CodEmp = nfc.CodEmp
        AND ipc.CodFil = nfc.CodFil
        AND ipc.NumNfc = nfc.NumNfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
    INNER JOIN E075PRO AS prod
        ON ipc.CodPro = prod.CodPro
    LEFT JOIN E660INC AS inc
        ON ipc.CodEmp = inc.CodEmp
        AND ipc.CodFil = inc.CodFil
        AND ipc.NumNfc = inc.NumNfi
        AND ipc.SeqIpc = inc.SeqIpc
    LEFT JOIN E640LCT AS lct
        ON nfc.CodEmp = lct.CodEmp
        AND nfc.CodFil = lct.CodFil
        AND nfc.DatEmi = lct.DatLct
    LEFT JOIN E045PLA AS pla
        ON lct.CtaDeb = pla.CtaRed
        AND lct.CodEmp = pla.CodEmp
WHERE nfc.DatGer BETWEEN '20250201' AND '20250228'
    AND inc.VlrIcm > 0
    AND lct.NumLct IS NOT NULL
ORDER BY nfc.DatEmi, ipc.NumNfc
```

</details>

---

### Bloco 3: Agregacões ⭐⭐

#### EX11: Totalizacao de Compras por Fornecedor no Trimestre

**Objetivo:** Agregar valores de compras por fornecedor para analise de volume de aquisicões

**Retornar:** Codigo Fornecedor, Nome Fornecedor, Quantidade de Notas Fiscais, Valor Total de Compras, Valor Médio por Nota, Valor Total de ICMS.

**Critérios:** Primeiro trimestre de 2025, agrupar por fornecedor, ordenar por valor total decrescente.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    fornec.CodFor AS codigo_fornecedor,
    fornec.NomFor AS nome_fornecedor,
    COUNT(DISTINCT nfc.NumNfc) AS quantidade_notas,
    SUM(nfc.VlrLiq) AS valor_total_compras,
    AVG(nfc.VlrLiq) AS valor_medio_nota,
    SUM(nfc.VlrIcm) AS valor_total_icms
FROM E440NFC AS nfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
WHERE nfc.DatEnt BETWEEN '20250101' AND '20250331'
GROUP BY fornec.CodFor, fornec.NomFor
ORDER BY valor_total_compras DESC
```

</details>

---

#### EX12: Analise Mensal de Impostos Recuperaveis por Filial

**Objetivo:** Consolidar valores de créditos tributarios (PIS e COFINS) por filial e mês

**Retornar:** Codigo Filial, Nome Filial, Mês/Ano, Quantidade de Itens, Total Base PIS, Total PIS Recuperavel, Total Base COFINS, Total COFINS Recuperavel.

**Critérios:** Primeiro semestre de 2025, agrupar por filial e mês, apenas itens com crédito.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    fil.CodFil AS codigo_filial,
    fil.NomFil AS nome_filial,
    MONTH(inc.DatGer) AS mes,
    YEAR(inc.DatGer) AS ano,
    COUNT(*) AS quantidade_itens,
    SUM(inc.VlrBpr) AS total_base_pis,
    SUM(inc.VlrPir) AS total_pis_recuperavel,
    SUM(inc.VlrBcr) AS total_base_cofins,
    SUM(inc.VlrCor) AS total_cofins_recuperavel
FROM E660INC AS inc
    INNER JOIN E070FIL AS fil
        ON inc.CodFil = fil.CodFil
        AND inc.CodEmp = fil.CodEmp
WHERE inc.DatGer BETWEEN '20250101' AND '20250630'
    AND (inc.VlrPir > 0 OR inc.VlrCor > 0)
GROUP BY fil.CodFil, fil.NomFil, MONTH(inc.DatGer), YEAR(inc.DatGer)
ORDER BY fil.CodFil, ano, mes
```

</details>

---

#### EX13: Ranking de Produtos Mais Comprados com Analise de Preco

**Objetivo:** Identificar os produtos com maior volume de compras e variacao de precos

**Retornar:** Codigo Produto, Descricao Produto, Quantidade Total Comprada, Quantidade de Fornecedores, Preco Mínimo, Preco Maximo, Preco Médio, Valor Total Gasto.

**Critérios:** Ano de 2025, agrupar por produto, mostrar apenas produtos com mais de 5 compras.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    prod.CodPro AS codigo_produto,
    prod.DesPro AS descricao_produto,
    SUM(ipc.QtdRec) AS quantidade_total_comprada,
    COUNT(DISTINCT nfc.CodFor) AS quantidade_fornecedores,
    MIN(ipc.PreUni) AS preco_minimo,
    MAX(ipc.PreUni) AS preco_maximo,
    AVG(ipc.PreUni) AS preco_medio,
    SUM(ipc.VlrLiq) AS valor_total_gasto
FROM E440IPC AS ipc
    INNER JOIN E440NFC AS nfc
        ON ipc.CodEmp = nfc.CodEmp
        AND ipc.CodFil = nfc.CodFil
        AND ipc.NumNfc = nfc.NumNfc
    INNER JOIN E075PRO AS prod
        ON ipc.CodPro = prod.CodPro
WHERE nfc.DatGer BETWEEN '20250101' AND '20251231'
GROUP BY prod.CodPro, prod.DesPro
HAVING COUNT(*) > 5
ORDER BY quantidade_total_comprada DESC
```

</details>

---

#### EX14: Consolidacao de Servicos por Centro de Custo com Analise Fiscal

**Objetivo:** Sumarizar gastos com servicos por centro de custo incluindo retencões tributarias

**Retornar:** Centro de Custo, Descricao Centro Custo, Quantidade de Notas, Valor Total Servicos, Total ISS Retido, Total IRRF Retido, Total PIS Retido, Total COFINS Retido.

**Critérios:** Primeiro semestre de 2025, agrupar por centro de custo, ordenar por valor total.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    ccu.CodCcu AS centro_custo,
    ccu.DesCcu AS descricao_centro_custo,
    COUNT(DISTINCT isc.NumNfc) AS quantidade_notas,
    SUM(isc.VlrLiq) AS valor_total_servicos,
    SUM(isc.VlrIss) AS total_iss_retido,
    SUM(isc.VlrIrf) AS total_irrf_retido,
    SUM(isc.VlrPit) AS total_pis_retido,
    SUM(isc.VlrCrt) AS total_cofins_retido
FROM E440ISC AS isc
    INNER JOIN E440NFC AS nfc
        ON isc.CodEmp = nfc.CodEmp
        AND isc.CodFil = nfc.CodFil
        AND isc.NumNfc = nfc.NumNfc
    LEFT JOIN E044CCU AS ccu
        ON isc.CodCcu = ccu.CodCcu
        AND isc.CodEmp = ccu.CodEmp
WHERE nfc.DatGer BETWEEN '20250101' AND '20250630'
GROUP BY ccu.CodCcu, ccu.DesCcu
ORDER BY valor_total_servicos DESC
```

</details>

---

#### EX15: Analise Comparativa de Compras: Produtos vs Servicos por Filial

**Objetivo:** Comparar volumes de compras de produtos e servicos por filial em uma única visao

**Retornar:** Codigo Filial, Nome Filial, Quantidade NF Produtos, Valor Total Produtos, Quantidade NF Servicos, Valor Total Servicos, Valor Total Geral, Percentual Produtos.

**Critérios:** Segundo trimestre de 2025, agrupar por filial, calcular participacao percentual.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    fil.CodFil AS codigo_filial,
    fil.NomFil AS nome_filial,
    COUNT(DISTINCT CASE WHEN nfc.TipNfe = 'P' THEN nfc.NumNfc END) AS qtd_nf_produtos,
    SUM(CASE WHEN nfc.TipNfe = 'P' THEN nfc.VlrBpr ELSE 0 END) AS valor_total_produtos,
    COUNT(DISTINCT CASE WHEN nfc.TipNfe = 'S' THEN nfc.NumNfc END) AS qtd_nf_servicos,
    SUM(CASE WHEN nfc.TipNfe = 'S' THEN nfc.VlrBse ELSE 0 END) AS valor_total_servicos,
    SUM(nfc.VlrLiq) AS valor_total_geral,
    CASE 
        WHEN SUM(nfc.VlrLiq) > 0 
        THEN ROUND((SUM(CASE WHEN nfc.TipNfe = 'P' THEN nfc.VlrBpr ELSE 0 END) / SUM(nfc.VlrLiq)) * 100, 2)
        ELSE 0 
    END AS percentual_produtos
FROM E440NFC AS nfc
    INNER JOIN E070FIL AS fil
        ON nfc.CodFil = fil.CodFil
        AND nfc.CodEmp = fil.CodEmp
WHERE nfc.DatEnt BETWEEN '20250401' AND '20250630'
GROUP BY fil.CodFil, fil.NomFil
ORDER BY valor_total_geral DESC
```

</details>

---

### Bloco 4: CTEs ⭐⭐⭐

#### EX16: CTE Basica - Totalizacao de Compras por Fornecedor com Filtro

**Objetivo:** Utilizar CTE para pré-calcular totais de compras e depois filtrar fornecedores relevantes

**Retornar:** Codigo Fornecedor, Nome Fornecedor, Quantidade de Notas, Valor Total de Compras, Ticket Médio, classificando apenas fornecedores com mais de 3 notas.

**Critérios:** Primeiro trimestre de 2025, valor total acima de R$ 10.000,00.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH TotaisCompras AS (
    SELECT
        fornec.CodFor AS codigo_fornecedor,
        fornec.NomFor AS nome_fornecedor,
        COUNT(DISTINCT nfc.NumNfc) AS quantidade_notas,
        SUM(nfc.VlrLiq) AS valor_total_compras,
        AVG(nfc.VlrLiq) AS ticket_medio
    FROM E440NFC AS nfc
        INNER JOIN E095FOR AS fornec
            ON nfc.CodFor = fornec.CodFor
    WHERE nfc.DatEnt BETWEEN '20250101' AND '20250331'
    GROUP BY fornec.CodFor, fornec.NomFor
)
SELECT
    codigo_fornecedor,
    nome_fornecedor,
    quantidade_notas,
    valor_total_compras,
    ticket_medio
FROM TotaisCompras
WHERE quantidade_notas > 3
    AND valor_total_compras > 10000
ORDER BY valor_total_compras DESC
```

</details>

---

#### EX17: CTEs Encadeadas - Analise de Produtos com Calculo de Participacao

**Objetivo:** Usar múltiplas CTEs para calcular totais gerais e depois percentual de participacao

**Retornar:** Codigo Produto, Descricao Produto, Quantidade Comprada, Valor Total, Percentual sobre Total Geral, Classificacao (A/B/C conforme participacao).

**Critérios:** Primeiro semestre de 2025, ordenar por valor decrescente.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH ComprasProdutos AS (
    SELECT
        prod.CodPro AS codigo_produto,
        prod.DesPro AS descricao_produto,
        SUM(ipc.QtdRec) AS quantidade_comprada,
        SUM(ipc.VlrLiq) AS valor_total
    FROM E440IPC AS ipc
        INNER JOIN E440NFC AS nfc
            ON ipc.CodEmp = nfc.CodEmp
            AND ipc.CodFil = nfc.CodFil
            AND ipc.NumNfc = nfc.NumNfc
        INNER JOIN E075PRO AS prod
            ON ipc.CodPro = prod.CodPro
    WHERE nfc.DatGer BETWEEN '20250101' AND '20250630'
    GROUP BY prod.CodPro, prod.DesPro
),
TotalGeral AS (
    SELECT SUM(valor_total) AS total_geral
    FROM ComprasProdutos
)
SELECT
    cp.codigo_produto,
    cp.descricao_produto,
    cp.quantidade_comprada,
    cp.valor_total,
    ROUND((cp.valor_total / tg.total_geral) * 100, 2) AS percentual_participacao,
    CASE
        WHEN (cp.valor_total / tg.total_geral) * 100 >= 10 THEN 'A'
        WHEN (cp.valor_total / tg.total_geral) * 100 >= 5 THEN 'B'
        ELSE 'C'
    END AS classificacao_abc
FROM ComprasProdutos cp
    CROSS JOIN TotalGeral tg
ORDER BY cp.valor_total DESC
```

</details>

---

#### EX18: CTE com Agregacao Temporal - Comparativo Mensal de Impostos

**Objetivo:** Criar CTE para agregar impostos por mês e depois calcular variacões

**Retornar:** Mês, Ano, Total Base ICMS, Total ICMS, Total IPI, Total PIS, Total COFINS, Carga Tributaria Efetiva (%).

**Critérios:** Primeiro semestre de 2025, agrupar por mês.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH ImpostosMensais AS (
    SELECT
        MONTH(nfc.DatEmi) AS mes,
        YEAR(nfc.DatEmi) AS ano,
        SUM(inc.VlrBic) AS total_base_icms,
        SUM(inc.VlrIcm) AS total_icms,
        SUM(inc.VlrIpi) AS total_ipi,
        SUM(inc.VlrPir) AS total_pis,
        SUM(inc.VlrCor) AS total_cofins,
        SUM(inc.VlrCtb) AS total_valor_contabil
    FROM E660INC AS inc
        INNER JOIN E660NFC AS nfc
            ON inc.CodFil = nfc.CodFil
            AND inc.CodFor = nfc.CodFor
            AND inc.NumNfi = nfc.NumNfi
    WHERE nfc.DatEmi BETWEEN '20250101' AND '20250630'
    GROUP BY MONTH(nfc.DatEmi), YEAR(nfc.DatEmi)
)
SELECT
    mes,
    ano,
    total_base_icms,
    total_icms,
    total_ipi,
    total_pis,
    total_cofins,
    (total_icms + total_ipi + total_pis + total_cofins) AS total_impostos,
    CASE
        WHEN total_valor_contabil > 0 
        THEN ROUND(((total_icms + total_ipi + total_pis + total_cofins) / total_valor_contabil) * 100, 2)
        ELSE 0
    END AS carga_tributaria_percentual
FROM ImpostosMensais
ORDER BY ano, mes
```

</details>

---

#### EX19: CTEs Múltiplas com JOINs - Analise Integrada de Performance por Filial

**Objetivo:** Combinar CTEs de produtos e servicos para visao consolidada por filial

**Retornar:** Codigo Filial, Nome Filial, Qtd NF Produtos, Valor Produtos, Qtd NF Servicos, Valor Servicos, Valor Total, Maior Fornecedor (nome), Valor Maior Fornecedor.

**Critérios:** Segundo trimestre de 2025.

---

#### EX20: CTE Complexa - Analise de Variacao Mês a Mês com Múltiplas Dimensões

**Objetivo:** Usar CTEs para calcular totais mensais e depois comparar com mês anterior

**Retornar:** Mês Atual, Ano, Valor Compras Mês Atual, Valor Mês Anterior, Variacao Absoluta, Variacao Percentual, Quantidade Fornecedores Ativos, Ticket Médio.

**Critérios:** Primeiro semestre de 2025, incluir analise comparativa.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH ComprasMensais AS (
    SELECT
        MONTH(nfc.DatEnt) AS mes,
        YEAR(nfc.DatEnt) AS ano,
        SUM(nfc.VlrLiq) AS valor_compras,
        COUNT(DISTINCT nfc.NumNfc) AS qtd_notas,
        COUNT(DISTINCT nfc.CodFor) AS qtd_fornecedores
    FROM E440NFC AS nfc
    WHERE nfc.DatEnt BETWEEN '20250101' AND '20250630'
        AND nfc.SitNfc = 'F'
    GROUP BY MONTH(nfc.DatEnt), YEAR(nfc.DatEnt)
),
ComparacaoMensal AS (
    SELECT
        cm.mes,
        cm.ano,
        cm.valor_compras,
        cm.qtd_notas,
        cm.qtd_fornecedores,
        LAG(cm.valor_compras) OVER (ORDER BY cm.ano, cm.mes) AS valor_mes_anterior,
        cm.valor_compras / NULLIF(cm.qtd_notas, 0) AS ticket_medio
    FROM ComprasMensais cm
)
SELECT
    mes,
    ano,
    valor_compras,
    valor_mes_anterior,
    (valor_compras - ISNULL(valor_mes_anterior, 0)) AS variacao_absoluta,
    CASE
        WHEN valor_mes_anterior > 0 
        THEN ROUND(((valor_compras - valor_mes_anterior) / valor_mes_anterior) * 100, 2)
        ELSE NULL
    END AS variacao_percentual,
    qtd_fornecedores AS fornecedores_ativos,
    ROUND(ticket_medio, 2) AS ticket_medio
FROM ComparacaoMensal
ORDER BY ano, mes
```

</details>

---

### Bloco 5: Window Functions ⭐⭐⭐

#### EX21: Window Function Basica - Numeracao Sequencial de Notas por Fornecedor

**Objetivo:** Adicionar numeracao sequencial as notas fiscais de cada fornecedor

**Retornar:** Codigo Fornecedor, Nome Fornecedor, Número NF, Data Entrada, Valor da Nota, Número Sequencial (particionado por fornecedor).

**Critérios:** Primeiro trimestre de 2025, ordenar por data de entrada.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    fornec.CodFor AS codigo_fornecedor,
    fornec.NomFor AS nome_fornecedor,
    nfc.NumNfc AS numero_nf,
    nfc.DatEnt AS data_entrada,
    nfc.VlrLiq AS valor_nota,
    ROW_NUMBER() OVER (PARTITION BY fornec.CodFor ORDER BY nfc.DatEnt) AS numero_sequencial
FROM E440NFC AS nfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
WHERE nfc.DatEnt BETWEEN '20250101' AND '20250331'
ORDER BY fornec.CodFor, nfc.DatEnt
```

</details>

---

#### EX22: Ranking Simples - Top Fornecedores por Valor Total

**Objetivo:** Criar ranking de fornecedores baseado no valor total de compras

**Retornar:** Posicao no Ranking, Codigo Fornecedor, Nome Fornecedor, Valor Total de Compras, Quantidade de Notas.

**Critérios:** Primeiro semestre de 2025, ordenar por valor decrescente.

<details>
<summary>📝 Ver Resposta</summary>

```sql
SELECT
    RANK() OVER (ORDER BY SUM(nfc.VlrLiq) DESC) AS posicao_ranking,
    fornec.CodFor AS codigo_fornecedor,
    fornec.NomFor AS nome_fornecedor,
    SUM(nfc.VlrLiq) AS valor_total_compras,
    COUNT(nfc.NumNfc) AS quantidade_notas
FROM E440NFC AS nfc
    INNER JOIN E095FOR AS fornec
        ON nfc.CodFor = fornec.CodFor
WHERE nfc.DatEnt BETWEEN '20250101' AND '20250630'
GROUP BY fornec.CodFor, fornec.NomFor
ORDER BY posicao_ranking
```

</details>

---

#### EX23: PARTITION BY Basico - Ranking de Produtos Mais Comprados por Filial

**Objetivo:** Classificar os produtos mais comprados dentro de cada filial

**Retornar:** Codigo Filial, Nome Filial, Codigo Produto, Descricao Produto, Quantidade Total, Ranking dentro da Filial.

**Critérios:** Primeiro trimestre de 2025, mostrar apenas top 3 por filial.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH ProdutosPorFilial AS (
    SELECT
        fil.CodFil AS codigo_filial,
        fil.NomFil AS nome_filial,
        prod.CodPro AS codigo_produto,
        prod.DesPro AS descricao_produto,
        SUM(ipc.QtdRec) AS quantidade_total,
        RANK() OVER (PARTITION BY fil.CodFil ORDER BY SUM(ipc.QtdRec) DESC) AS ranking_filial
    FROM E440IPC AS ipc
        INNER JOIN E440NFC AS nfc
            ON ipc.CodEmp = nfc.CodEmp
            AND ipc.CodFil = nfc.CodFil
            AND ipc.NumNfc = nfc.NumNfc
        INNER JOIN E070FIL AS fil
            ON nfc.CodFil = fil.CodFil
            AND nfc.CodEmp = fil.CodEmp
        INNER JOIN E075PRO AS prod
            ON ipc.CodPro = prod.CodPro
    WHERE nfc.DatGer BETWEEN '20250101' AND '20250331'
    GROUP BY fil.CodFil, fil.NomFil, prod.CodPro, prod.DesPro
)
SELECT
    codigo_filial,
    nome_filial,
    ranking_filial,
    codigo_produto,
    descricao_produto,
    quantidade_total
FROM ProdutosPorFilial
WHERE ranking_filial <= 3
ORDER BY codigo_filial, ranking_filial
```

</details>

---

#### EX24: Funcões LAG e LEAD - Comparacao com Mês Anterior e Posterior

**Objetivo:** Calcular valor mensal de compras e comparar com meses adjacentes

**Retornar:** Mês, Ano, Valor do Mês, Valor Mês Anterior, Valor Mês Seguinte, Variacao vs Mês Anterior (valor absoluto).

**Critérios:** Primeiro semestre de 2025.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH ComprasMensais AS (
    SELECT
        MONTH(nfc.DatEnt) AS mes,
        YEAR(nfc.DatEnt) AS ano,
        SUM(nfc.VlrLiq) AS valor_mes
    FROM E440NFC AS nfc
    WHERE nfc.DatEnt BETWEEN '20250101' AND '20250630'
    GROUP BY MONTH(nfc.DatEnt), YEAR(nfc.DatEnt)
)
SELECT
    mes,
    ano,
    valor_mes,
    LAG(valor_mes) OVER (ORDER BY ano, mes) AS valor_mes_anterior,
    LEAD(valor_mes) OVER (ORDER BY ano, mes) AS valor_mes_seguinte,
    valor_mes - LAG(valor_mes) OVER (ORDER BY ano, mes) AS variacao_mes_anterior
FROM ComprasMensais
ORDER BY ano, mes
```

</details>

---

#### EX25: SUM OVER - Total Acumulado e Participacao Percentual

**Objetivo:** Calcular valor acumulado mês a mês e percentual sobre o total do período

**Retornar:** Mês, Ano, Valor Mensal, Acumulado até o Mês, Percentual sobre Total do Semestre, Total do Semestre.

**Critérios:** Primeiro semestre de 2025.

<details>
<summary>📝 Ver Resposta</summary>

```sql
WITH ComprasMensais AS (
    SELECT
        MONTH(nfc.DatEnt) AS mes,
        YEAR(nfc.DatEnt) AS ano,
        SUM(nfc.VlrLiq) AS valor_mensal
    FROM E440NFC AS nfc
    WHERE nfc.DatEnt BETWEEN '20250101' AND '20250630'
    GROUP BY MONTH(nfc.DatEnt), YEAR(nfc.DatEnt)
)
SELECT
    mes,
    ano,
    valor_mensal,
    SUM(valor_mensal) OVER (ORDER BY ano, mes) AS valor_acumulado,
    SUM(valor_mensal) OVER () AS total_semestre,
    ROUND((valor_mensal / SUM(valor_mensal) OVER ()) * 100, 2) AS percentual_total
FROM ComprasMensais
ORDER BY ano, mes
```

</details>

---

### Bloco 6: Complementares (UNION, Subconsultas, DML) ⭐⭐⭐

#### EX26: UNION Basico - Listar Todos os Itens de Compra

**Objetivo:** Combinar produtos e servicos em uma única lista usando UNION

**Retornar:** Tipo (Produto ou Servico), Codigo do Item, Número da NF, Valor.

**Critérios:** Mês de Janeiro 2025, ordenar por tipo e valor.

---

#### EX27: Subconsulta Simples - Notas Acima da Média

**Objetivo:** Usar subconsulta para filtrar notas fiscais com valor acima da média do período

**Retornar:** Codigo Filial, Número NF, Codigo Fornecedor, Valor da Nota, Média do Período.

**Critérios:** Primeiro trimestre de 2025.

---

#### EX28: EXISTS Basico - Fornecedores com Compras no Período

**Objetivo:** Identificar fornecedores que tiveram pelo menos uma compra no período usando EXISTS

**Retornar:** Codigo Fornecedor, Nome Fornecedor, Cidade, UF.

**Critérios:** Fornecedores com compras em Janeiro 2025.

---

#### EX29: Funcões de String e Data Basicas - Formatacao de Fornecedores

**Objetivo:** Aplicar funcões de string e data para formatar e filtrar informacões

**Retornar:** Codigo Fornecedor, Nome em Maiúsculo, Primeiros 20 caracteres do Nome, Data da Última Compra, Dias desde a Última Compra.

**Critérios:** Fornecedores que compraram nos últimos 90 dias, nome contém "COMERCIO".

---

#### EX30: INSERT SELECT Basico - Tabela Resumo de Fornecedores

**Objetivo:** Criar tabela temporaria e inserir dados resumidos usando INSERT SELECT

**Retornar:** Tabela com totais por fornecedor para analise.

**Critérios:** Janeiro 2025, criar resumo simples.

---

### 6.2. Exercícios Python para Analise de Dados

---

#### 6.2.1. Bloco 1: Sintaxe Basica

##### Exercício 1.1 - Configuracões de DAG 🟢
**Contexto:** Toda DAG do Airflow precisa de configuracões basicas.  
**Objetivo:** Trabalhar com dicionarios e tipos de dados.
```python
# Configuracao de uma DAG
dag_config = {
    "dag_id": "process_sales",
    "schedule_interval": "0 8 * * *",
    "retries": 3,
    "active": True,
    "tags": ["vendas", "diario"]
}

# TODO:
# 1. Imprimir o nome da DAG
# 2. Verificar se a DAG esta ativa (True/False)
# 3. Adicionar uma nova tag "producao" a lista de tags
# 4. Alterar o número de retries para 5
# 5. Imprimir a configuracao completa
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Nome da DAG
print(f"DAG: {dag_config['dag_id']}")  # DAG: process_sales

# 2. Verificar se esta ativa
print(f"Ativa: {dag_config['active']}")  # Ativa: True

# 3. Adicionar tag
dag_config['tags'].append('producao')
print(f"Tags: {dag_config['tags']}")  # ['vendas', 'diario', 'producao']

# 4. Alterar retries
dag_config['retries'] = 5

# 5. Configuracao completa
print(dag_config)
```

**Conceitos aplicados:**
- Dicionarios (acesso por chave)
- Tipos de dados (string, int, bool, list)
- Métodos de lista (`.append()`)

</details>

---

##### Exercício 1.2 - Lista de Tabelas para Processar 🟢
**Contexto:** Em pipelines, frequentemente processamos múltiplas tabelas.  
**Objetivo:** Manipular listas e usar loops basicos.
```python
# Tabelas a serem extraídas do SQL Server
tabelas = ["e095for", "e440nfc", "e660inv", "e640lct", "e045pla"]

# TODO:
# 1. Contar quantas tabelas existem
# 2. Imprimir a primeira e a última tabela
# 3. Verificar se "e640lct" esta na lista
# 4. Adicionar a tabela "e075age" ao final
# 5. Criar um loop que imprima: "Processando tabela: [nome]"
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Contar tabelas
print(f"Total: {len(tabelas)} tabelas")  # Total: 5 tabelas

# 2. Primeira e última
print(f"Primeira: {tabelas[0]}")   # Primeira: e095for
print(f"Última: {tabelas[-1]}")    # Última: e045pla

# 3. Verificar se existe
existe = "e640lct" in tabelas
print(f"e640lct existe: {existe}")  # True

# 4. Adicionar tabela
tabelas.append("e075age")

# 5. Loop para processar
for tabela in tabelas:
    print(f"Processando tabela: {tabela}")
```

**Conceitos aplicados:**
- Listas (indexacao, tamanho)
- Operador `in` para verificacao
- Loop `for` basico
- f-strings para formatacao

</details>

---

##### Exercício 1.3 - Validacao de Schemas dbt 🟡
**Contexto:** No dbt, organizamos modelos em schemas (raw, staging, marts).  
**Objetivo:** Usar condicionais e estruturas de controle.

```python
# Modelo dbt e seu schema
modelo = "stg_fornecedores"
schema_atual = "staging"

# Regras de nomenclatura:
# - Modelos "raw_" devem estar em schema "raw"
# - Modelos "stg_" devem estar em schema "staging"
# - Modelos "fct_" ou "dim_" devem estar em schema "marts"

# TODO:
# 1. Extrair o prefixo do modelo (parte antes do "_")
# 2. Determinar o schema esperado baseado no prefixo
# 3. Verificar se o schema atual esta correto
# 4. Imprimir mensagem de validacao
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Extrair prefixo
prefixo = modelo.split("_")[0]
print(f"Prefixo: {prefixo}")  # Prefixo: stg

# 2. Determinar schema esperado
if prefixo == "raw":
    schema_esperado = "raw"
elif prefixo == "stg":
    schema_esperado = "staging"
elif prefixo in ["fct", "dim"]:
    schema_esperado = "marts"
else:
    schema_esperado = "desconhecido"

print(f"Schema esperado: {schema_esperado}")  # staging

# 3 e 4. Validar
if schema_atual == schema_esperado:
    print(f"✓ Modelo '{modelo}' esta no schema correto: {schema_atual}")
else:
    print(f"✗ ERRO: Modelo '{modelo}' deveria estar em '{schema_esperado}', mas esta em '{schema_atual}'")
```

**Conceitos aplicados:**
- String methods (`.split()`)
- Condicionais (`if/elif/else`)
- Operador `in` para múltiplos valores
- Comparacao de igualdade

</details>

---

##### Exercício 1.4 - Filtragem de Tabelas por Critério 🟡
**Contexto:** as vezes queremos processar apenas tabelas específicas.  
**Objetivo:** Combinar listas, loops e condicionais.

```python
# Todas as tabelas disponíveis
todas_tabelas = [
    "e095for", "e440nfc", "e660inv", "e640lct", 
    "e045pla", "e075age", "e001tns", "e070emp"
]

# Critérios de filtro
processar_apenas = ["e095for", "e640lct", "e045pla"]  # Lista específica
iniciam_com = "e0"  # Prefixo

# TODO:
# 1. Criar lista com tabelas que estao em 'processar_apenas'
# 2. Criar lista com tabelas que comecam com 'e0'
# 3. Criar lista com tabelas que atendem AMBOS os critérios
# 4. Imprimir quantas tabelas foram selecionadas em cada caso
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Tabelas na lista específica
tabelas_especificas = []
for tabela in todas_tabelas:
    if tabela in processar_apenas:
        tabelas_especificas.append(tabela)

print(f"Específicas: {tabelas_especificas}")
# ['e095for', 'e640lct', 'e045pla']

# 2. Tabelas que comecam com 'e0'
tabelas_prefixo = []
for tabela in todas_tabelas:
    if tabela.startswith(iniciam_com):
        tabelas_prefixo.append(tabela)

print(f"Com prefixo 'e0': {tabelas_prefixo}")
# ['e095for', 'e045pla', 'e075age', 'e001tns', 'e070emp']

# 3. Tabelas que atendem AMBOS
tabelas_ambos = []
for tabela in todas_tabelas:
    if tabela in processar_apenas and tabela.startswith(iniciam_com):
        tabelas_ambos.append(tabela)

print(f"Ambos critérios: {tabelas_ambos}")
# ['e095for', 'e045pla']

# 4. Contagem
print(f"\nResumo:")
print(f"- Específicas: {len(tabelas_especificas)} tabelas")
print(f"- Com prefixo: {len(tabelas_prefixo)} tabelas")
print(f"- Ambos: {len(tabelas_ambos)} tabelas")
```

**Conceitos aplicados:**
- Loops aninhados com condicionais
- String method (`.startswith()`)
- Operadores logicos (`and`)
- Listas vazias e `.append()`

</details>

---

##### Exercício 1.5 - Construtor de Task IDs Dinâmicos 🔴
**Contexto:** No Airflow, cada task precisa de um ID único. Ao processar múltiplas tabelas, geramos IDs automaticamente.  
**Objetivo:** Combinar loops, strings, dicionarios e logica condicional.

```python
# Tabelas e suas configuracões
tabelas_config = {
    "e095for": {"tipo": "incremental", "schema": "raw"},
    "e440nfc": {"tipo": "full", "schema": "raw"},
    "e640lct": {"tipo": "incremental", "schema": "staging"},
    "e045pla": {"tipo": "full", "schema": "marts"}
}

# Padrao de task_id: {tipo}_{schema}_{tabela}
# Exemplo: "incremental_raw_e095for"

# TODO:
# 1. Criar dicionario onde a chave é o task_id e o valor é o nome da tabela
# 2. Criar lista apenas com task_ids de tabelas incrementais
# 3. Contar quantas tasks existem por schema
# 4. Imprimir relatorio formatado com todas as informacões
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Dicionario de task_ids
task_mapping = {}
for tabela, config in tabelas_config.items():
    task_id = f"{config['tipo']}_{config['schema']}_{tabela}"
    task_mapping[task_id] = tabela

print("Task IDs criados:")
for task_id, tabela in task_mapping.items():
    print(f"  {task_id} -> {tabela}")

# 2. Tasks incrementais apenas
tasks_incrementais = []
for tabela, config in tabelas_config.items():
    if config['tipo'] == 'incremental':
        task_id = f"{config['tipo']}_{config['schema']}_{tabela}"
        tasks_incrementais.append(task_id)

print(f"\nTasks incrementais: {tasks_incrementais}")

# 3. Contar por schema
contador_schema = {}
for tabela, config in tabelas_config.items():
    schema = config['schema']
    if schema in contador_schema:
        contador_schema[schema] += 1
    else:
        contador_schema[schema] = 1

print(f"\nTasks por schema: {contador_schema}")

# 4. Relatorio completo
print("\n" + "="*50)
print("RELAToRIO DE TASKS")
print("="*50)
print(f"Total de tasks: {len(task_mapping)}")
print(f"Tasks incrementais: {len(tasks_incrementais)}")
print(f"Tasks full refresh: {len(task_mapping) - len(tasks_incrementais)}")
print("\nDistribuicao por schema:")
for schema, count in contador_schema.items():
    print(f"  - {schema}: {count} tasks")
```

**Conceitos aplicados:**
- Dicionarios aninhados (`.items()`)
- Criacao dinâmica de chaves em dicionario
- Contadores com dicionario
- Múltiplos loops independentes
- Formatacao de strings complexa
- Operacões aritméticas com contadores

</details>

---

#### 6.2.2. Bloco 2: Recursos da Linguagem

##### Exercício 2.1 - List Comprehension para Task IDs 🟢
**Contexto:** Gerar listas de forma concisa é essencial em Python.  
**Objetivo:** Transformar loops em list comprehensions.

```python
# Lista de tabelas
tabelas = ["e095for", "e440nfc", "e660inv", "e640lct", "e045pla"]

# TODO (usando list comprehension):
# 1. Criar lista de task_ids no formato "extract_{tabela}"
# 2. Criar lista apenas com tabelas que comecam com "e6"
# 3. Criar lista com o tamanho (length) de cada nome de tabela
# 4. Criar lista de tuplas (tabela, tamanho)
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Task IDs
task_ids = [f"extract_{tabela}" for tabela in tabelas]
print(task_ids)
# ['extract_e095for', 'extract_e440nfc', 'extract_e660inv', 'extract_e640lct', 'extract_e045pla']

# 2. Filtrar por prefixo
tabelas_e6 = [t for t in tabelas if t.startswith("e6")]
print(tabelas_e6)
# ['e660inv', 'e640lct']

# 3. Tamanhos
tamanhos = [len(t) for t in tabelas]
print(tamanhos)
# [7, 7, 7, 7, 7]

# 4. Tuplas (tabela, tamanho)
tabelas_tamanhos = [(t, len(t)) for t in tabelas]
print(tabelas_tamanhos)
# [('e095for', 7), ('e440nfc', 7), ('e660inv', 7), ('e640lct', 7), ('e045pla', 7)]
```

**Conceitos aplicados:**
- List comprehension basica
- List comprehension com filtro (`if`)
- Expressões dentro de comprehension
- Criacao de tuplas em comprehension

</details>

---

##### Exercício 2.2 - Dictionary Comprehension para Configs 🟢
**Contexto:** Criar dicionarios de configuracao rapidamente.  
**Objetivo:** Usar dictionary comprehension.

```python
# Lista de modelos dbt
modelos = ["stg_fornecedores", "stg_produtos", "stg_vendas"]

# Configuracao padrao
config_padrao = {
    "materialized": "view",
    "schema": "staging"
}

# TODO (usando dictionary comprehension):
# 1. Criar dicionario {modelo: "view"} para cada modelo
# 2. Criar dicionario {modelo: len(modelo)} com tamanho do nome
# 3. Criar dicionario {modelo: config_padrao} para cada modelo
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Modelo -> materializacao
materializacoes = {modelo: "view" for modelo in modelos}
print(materializacoes)
# {'stg_fornecedores': 'view', 'stg_produtos': 'view', 'stg_vendas': 'view'}

# 2. Modelo -> tamanho
tamanhos = {modelo: len(modelo) for modelo in modelos}
print(tamanhos)
# {'stg_fornecedores': 17, 'stg_produtos': 12, 'stg_vendas': 10}

# 3. Modelo -> config completa
configs = {modelo: config_padrao.copy() for modelo in modelos}
print(configs)
# {'stg_fornecedores': {'materialized': 'view', 'schema': 'staging'}, ...}

# Nota: .copy() é importante para nao compartilhar a mesma referência
```

**Conceitos aplicados:**
- Dictionary comprehension basica
- Expressões em dictionary comprehension
- `.copy()` para dicionarios (referência vs copia)

</details>

---

##### Exercício 2.3 - Simulando o Decorator @task 🟡
**Contexto:** O Airflow usa `@task` para transformar funcões em tasks.  
**Objetivo:** Entender o conceito de decorators.

```python
# Simulacao simplificada do comportamento do @task do Airflow

def criar_task(func):
    """Decorator que adiciona metadados a funcao"""
    def wrapper(*args, **kwargs):
        print(f"[TASK START] Executando: {func.__name__}")
        resultado = func(*args, **kwargs)
        print(f"[TASK END] Finalizado: {func.__name__}")
        return resultado
    
    wrapper.task_id = func.__name__
    return wrapper

# TODO:
# 1. Criar funcao 'extrair_dados' que retorna "dados extraídos"
# 2. Aplicar o decorator 'criar_task' nela usando @
# 3. Criar funcao 'transformar_dados' que recebe dados e retorna f"{dados} transformados"
# 4. Aplicar o decorator também
# 5. Executar ambas e observar os logs
```

<details>
<summary>📝 Ver Solucao</summary>

```python
@criar_task
def extrair_dados():
    return "dados extraídos"

@criar_task
def transformar_dados(dados):
    return f"{dados} transformados"

# Executando as tasks
print("\n--- Execucao do Pipeline ---")
dados = extrair_dados()
print(f"Resultado: {dados}\n")

resultado_final = transformar_dados(dados)
print(f"Resultado: {resultado_final}\n")

# Acessando metadados
print(f"Task ID 1: {extrair_dados.task_id}")
print(f"Task ID 2: {transformar_dados.task_id}")

# Output:
# --- Execucao do Pipeline ---
# [TASK START] Executando: extrair_dados
# [TASK END] Finalizado: extrair_dados
# Resultado: dados extraídos
#
# [TASK START] Executando: transformar_dados
# [TASK END] Finalizado: transformar_dados
# Resultado: dados extraídos transformados
#
# Task ID 1: extrair_dados
# Task ID 2: transformar_dados
```

**Conceitos aplicados:**
- Decorators (conceito e sintaxe `@`)
- Funcões como objetos
- `*args` e `**kwargs`
- Atributos de funcões

</details>

---

##### Exercício 2.4 - Context Manager para Conexões 🟡
**Contexto:** Gerenciar conexões com bancos requer abertura e fechamento adequados.  
**Objetivo:** Usar context managers (`with`).

```python
class ConexaoSimulada:
    """Simula uma conexao de banco de dados"""
    
    def __init__(self, nome_conexao):
        self.nome = nome_conexao
        self.conectado = False
    
    def __enter__(self):
        """Executado ao entrar no bloco 'with'"""
        print(f"[CONNECT] Abrindo conexao: {self.nome}")
        self.conectado = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Executado ao sair do bloco 'with'"""
        print(f"[DISCONNECT] Fechando conexao: {self.nome}")
        self.conectado = False
    
    def executar(self, query):
        if self.conectado:
            print(f"[QUERY] Executando: {query}")
            return f"Resultado de '{query}'"
        else:
            return "ERRO: Conexao fechada"

# TODO:
# 1. Usar context manager para conectar ao "redshift"
# 2. Executar query "SELECT * FROM tabela"
# 3. Tentar executar query fora do bloco 'with' e observar erro
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1 e 2. Usando context manager corretamente
print("=== Uso correto ===")
with ConexaoSimulada("redshift") as conn:
    resultado = conn.executar("SELECT * FROM tabela")
    print(f"Retorno: {resultado}")
    print(f"Status dentro: conectado={conn.conectado}")

print(f"Status fora: conectado={conn.conectado}\n")

# 3. Tentando usar fora do contexto (erro)
print("=== Uso incorreto ===")
conexao = ConexaoSimulada("redshift")
print(conexao.executar("SELECT * FROM outra_tabela"))

# Output:
# === Uso correto ===
# [CONNECT] Abrindo conexao: redshift
# [QUERY] Executando: SELECT * FROM tabela
# Retorno: Resultado de 'SELECT * FROM tabela'
# Status dentro: conectado=True
# [DISCONNECT] Fechando conexao: redshift
# Status fora: conectado=False
#
# === Uso incorreto ===
# ERRO: Conexao fechada
```

**Conceitos aplicados:**
- Context managers (`with`)
- `__enter__` e `__exit__`
- Gerenciamento automatico de recursos
- Estados de objetos

</details>

---

##### Exercício 2.5 - Pipeline Completo com Múltiplos Conceitos 🔴
**Contexto:** Construir um pipeline que combina todos os conceitos aprendidos.  
**Objetivo:** Integrar comprehensions, decorators e context managers.

```python
# Decorator para logging
def log_task(func):
    def wrapper(*args, **kwargs):
        print(f"→ Iniciando: {func.__name__}")
        resultado = func(*args, **kwargs)
        print(f"✓ Concluído: {func.__name__}")
        return resultado
    return wrapper

# Context manager (ja definido anteriormente)
class ConexaoSimulada:
    def __init__(self, nome):
        self.nome = nome
    
    def __enter__(self):
        print(f"[DB] Conectando a {self.nome}")
        return self
    
    def __exit__(self, *args):
        print(f"[DB] Desconectando de {self.nome}")
    
    def buscar_tabelas(self):
        return ["e095for", "e440nfc", "e640lct"]

# TODO:
# 1. Criar funcao decorada 'obter_tabelas_filtradas' que:
#    - Recebe uma conexao e um prefixo
#    - Busca tabelas da conexao
#    - Retorna apenas tabelas que comecam com o prefixo (usar comprehension)
#
# 2. Criar funcao decorada 'gerar_task_ids' que:
#    - Recebe lista de tabelas
#    - Retorna dicionario {tabela: task_id} usando dict comprehension
#    - task_id formato: "process_{tabela}"
#
# 3. Executar pipeline completo:
#    - Abrir conexao com context manager
#    - Filtrar tabelas (prefixo "e6")
#    - Gerar task_ids
#    - Imprimir resultado
```

<details>
<summary>📝 Ver Solucao</summary>

```python
@log_task
def obter_tabelas_filtradas(conexao, prefixo):
    todas = conexao.buscar_tabelas()
    filtradas = [t for t in todas if t.startswith(prefixo)]
    return filtradas

@log_task
def gerar_task_ids(tabelas):
    return {tabela: f"process_{tabela}" for tabela in tabelas}

# Pipeline completo
print("="*50)
print("EXECUTANDO PIPELINE")
print("="*50)

with ConexaoSimulada("mssql_source") as db:
    # Etapa 1: Filtrar tabelas
    tabelas = obter_tabelas_filtradas(db, "e6")
    print(f"Tabelas filtradas: {tabelas}\n")
    
    # Etapa 2: Gerar task_ids
    tasks = gerar_task_ids(tabelas)
    print(f"Tasks geradas:")
    for tabela, task_id in tasks.items():
        print(f"  {tabela} → {task_id}")

print("\n" + "="*50)
print("PIPELINE FINALIZADO")
print("="*50)

# Output:
# ==================================================
# EXECUTANDO PIPELINE
# ==================================================
# [DB] Conectando a mssql_source
# → Iniciando: obter_tabelas_filtradas
# ✓ Concluído: obter_tabelas_filtradas
# Tabelas filtradas: ['e640lct']
#
# → Iniciando: gerar_task_ids
# ✓ Concluído: gerar_task_ids
# Tasks geradas:
#   e640lct → process_e640lct
# [DB] Desconectando de mssql_source
#
# ==================================================
# PIPELINE FINALIZADO
# ==================================================
```

**Conceitos aplicados:**
- Múltiplos decorators
- List comprehension com filtro
- Dictionary comprehension
- Context manager
- Composicao de funcões
- Pipeline de dados

</details>

---

#### 6.2.3. Bloco 3: Pandas para Analise Exploratoria

##### Exercício 3.1 - Lendo CSV de Configuracao 🟢
**Contexto:** Arquivos CSV frequentemente armazenam configuracões de pipelines.  
**Objetivo:** Ler e explorar DataFrames basicos.

```python
import pandas as pd
from io import StringIO

# Simulando um CSV de dependências (como nrt_dependencies.csv)
csv_data = """tabela_origem;tabela_destino;tipo_carga
e095for;stg_fornecedores;incremental
e440nfc;stg_notas_fiscais;incremental
e640lct;stg_lancamentos;full
e045pla;stg_plano_contas;full
e660inv;stg_inventario;incremental"""

# TODO:
# 1. Ler o CSV em um DataFrame (sep=";")
# 2. Exibir as primeiras 3 linhas
# 3. Exibir informacões sobre o DataFrame (.info())
# 4. Exibir os nomes das colunas
# 5. Contar quantas linhas existem
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Ler CSV
df = pd.read_csv(StringIO(csv_data), sep=";")

# 2. Primeiras 3 linhas
print("Primeiras 3 linhas:")
print(df.head(3))
print()

# 3. Informacões
print("Informacões do DataFrame:")
df.info()
print()

# 4. Nomes das colunas
print(f"Colunas: {list(df.columns)}")
# ['tabela_origem', 'tabela_destino', 'tipo_carga']

# 5. Contagem
print(f"Total de linhas: {len(df)}")  # 5
```

**Conceitos aplicados:**
- `pd.read_csv()` com separador customizado
- `.head()` para preview
- `.info()` para metadados
- `.columns` para nomes de colunas
- `len()` para contar linhas

</details>

---

##### Exercício 3.2 - Filtragem de Dados 🟢
**Contexto:** Filtrar configuracões por tipo de carga é comum.  
**Objetivo:** Usar filtros booleanos em DataFrames.

```python
import pandas as pd
from io import StringIO

csv_data = """tabela_origem;tabela_destino;tipo_carga;schema
e095for;stg_fornecedores;incremental;staging
e440nfc;stg_notas_fiscais;incremental;staging
e640lct;stg_lancamentos;full;staging
e045pla;dim_plano_contas;full;marts
e660inv;stg_inventario;incremental;staging"""

df = pd.read_csv(StringIO(csv_data), sep=";")

# TODO:
# 1. Filtrar apenas linhas com tipo_carga = "incremental"
# 2. Filtrar apenas linhas do schema "staging"
# 3. Filtrar linhas incremental E staging (ambos)
# 4. Contar quantas linhas têm tipo "full"
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Apenas incrementais
incrementais = df[df['tipo_carga'] == 'incremental']
print("Incrementais:")
print(incrementais)
print()

# 2. Apenas staging
staging = df[df['schema'] == 'staging']
print("Schema staging:")
print(staging)
print()

# 3. Incremental E staging
inc_stg = df[(df['tipo_carga'] == 'incremental') & (df['schema'] == 'staging')]
print("Incremental + Staging:")
print(inc_stg)
print()

# 4. Contar tipo "full"
count_full = len(df[df['tipo_carga'] == 'full'])
print(f"Quantidade de tipo 'full': {count_full}")  # 2
```

**Conceitos aplicados:**
- Filtros booleanos (`df[condicao]`)
- Operadores de comparacao (`==`)
- Operadores logicos (`&` para AND)
- Parênteses em filtros múltiplos

</details>

---

##### Exercício 3.3 - Selecao e Transformacao de Colunas 🟡
**Contexto:** Preparar dados para gerar tasks dinamicamente.  
**Objetivo:** Selecionar colunas e criar novas.

```python
import pandas as pd
from io import StringIO

csv_data = """tabela_origem;tabela_destino;tipo_carga;schema
e095for;stg_fornecedores;incremental;staging
e440nfc;stg_notas_fiscais;incremental;staging
e640lct;stg_lancamentos;full;staging
e045pla;dim_plano_contas;full;marts"""

df = pd.read_csv(StringIO(csv_data), sep=";")

# TODO:
# 1. Selecionar apenas colunas 'tabela_origem' e 'tipo_carga'
# 2. Criar nova coluna 'task_id' no formato: "{tipo_carga}_{tabela_origem}"
# 3. Criar nova coluna 'prefixo' com os 3 primeiros caracteres de tabela_origem
# 4. Exibir o DataFrame final com todas as colunas
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Selecionar colunas
df_selecionado = df[['tabela_origem', 'tipo_carga']]
print("Colunas selecionadas:")
print(df_selecionado)
print()

# 2. Criar coluna task_id
df['task_id'] = df['tipo_carga'] + '_' + df['tabela_origem']

# 3. Criar coluna prefixo
df['prefixo'] = df['tabela_origem'].str[:3]

# 4. DataFrame completo
print("DataFrame com novas colunas:")
print(df)

# Output:
#   tabela_origem  tabela_destino     tipo_carga  schema                    task_id prefixo
# 0       e095for  stg_fornecedores  incremental staging  incremental_e095for    e09
# 1       e440nfc  stg_notas_fiscais incremental staging  incremental_e440nfc    e44
# 2       e640lct  stg_lancamentos          full staging          full_e640lct    e64
# 3       e045pla  dim_plano_contas        full   marts          full_e045pla    e04
```

**Conceitos aplicados:**
- Selecao de colunas (`df[['col1', 'col2']]`)
- Criacao de colunas com operacões
- Concatenacao de strings em Series
- `.str` accessor para métodos de string

</details>

---

##### Exercício 3.4 - Agrupamento e Agregacao 🟡
**Contexto:** Analisar distribuicao de tasks por schema ou tipo.  
**Objetivo:** Usar `groupby()` e agregacões.
```python
import pandas as pd
from io import StringIO

csv_data = """tabela;schema;tipo_carga;prioridade
e095for;staging;incremental;1
e440nfc;staging;incremental;1
e640lct;staging;full;2
e045pla;marts;full;2
e660inv;staging;incremental;1
e001tns;raw;full;3
e070emp;raw;incremental;2"""

df = pd.read_csv(StringIO(csv_data), sep=";")

# TODO:
# 1. Contar quantas tabelas existem por schema
# 2. Contar quantas tabelas existem por tipo_carga
# 3. Calcular a prioridade média por schema
# 4. Criar tabela cruzada: schema vs tipo_carga (contagem)
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Contagem por schema
por_schema = df.groupby('schema')['tabela'].count()
print("Tabelas por schema:")
print(por_schema)
print()
# staging    4
# marts      1
# raw        2

# 2. Contagem por tipo_carga
por_tipo = df.groupby('tipo_carga')['tabela'].count()
print("Tabelas por tipo_carga:")
print(por_tipo)
print()
# full           3
# incremental    4

# 3. Prioridade média por schema
prioridade_media = df.groupby('schema')['prioridade'].mean()
print("Prioridade média por schema:")
print(prioridade_media)
print()
# staging    1.25
# marts      2.00
# raw        2.50

# 4. Tabela cruzada
tabela_cruzada = pd.crosstab(df['schema'], df['tipo_carga'])
print("Schema vs Tipo (contagem):")
print(tabela_cruzada)
# tipo_carga   full  incremental
# schema                        
# marts           1            0
# raw             1            1
# staging         1            3
```

**Conceitos aplicados:**
- `.groupby()` para agrupamento
- Agregacões (`.count()`, `.mean()`)
- `pd.crosstab()` para tabelas cruzadas
- Analise multidimensional

</details>

---

##### Exercício 3.5 - Iteracao para Geracao de Tasks 🔴
**Contexto:** Converter DataFrame em estruturas para criar tasks do Airflow dinamicamente.  
**Objetivo:** Iterar sobre DataFrame e construir dicionarios/listas complexas.

```python
import pandas as pd
from io import StringIO

csv_data = """tabela_origem;tabela_destino;tipo_carga;schema;dependencias
e095for;stg_fornecedores;incremental;staging;
e440nfc;stg_notas_fiscais;incremental;staging;e095for
e640lct;stg_lancamentos;full;staging;e440nfc
e045pla;dim_plano_contas;full;marts;e640lct;e095for
e660inv;fct_inventario;incremental;marts;e045pla;e440nfc"""

df = pd.read_csv(StringIO(csv_data), sep=";")

# TODO:
# 1. Converter DataFrame em lista de dicionarios
# 2. Para cada linha, criar estrutura:
#    {
#        "task_id": "{tipo_carga}_{tabela_origem}",
#        "source": "tabela_origem",
#        "target": "tabela_destino",
#        "dependencies": ["dep1", "dep2"]  # split por ";"
#    }
# 3. Filtrar apenas tasks do schema "staging"
# 4. Criar dicionario final: {task_id: task_config}
# 5. Imprimir em formato legível
```

<details>
<summary>📝 Ver Solucao</summary>

```python
# 1. Converter para lista de dicionarios
registros = df.to_dict('records')

# 2. Criar estrutura de tasks
tasks = []
for row in registros:
    # Processar dependências (pode ser NaN ou string com ";")
    deps_raw = row['dependencias']
    if pd.isna(deps_raw) or deps_raw == '':
        dependencias = []
    else:
        dependencias = [d.strip() for d in str(deps_raw).split(';')]
    
    task = {
        "task_id": f"{row['tipo_carga']}_{row['tabela_origem']}",
        "source": row['tabela_origem'],
        "target": row['tabela_destino'],
        "schema": row['schema'],
        "dependencies": dependencias
    }
    tasks.append(task)

# 3. Filtrar schema "staging"
tasks_staging = [t for t in tasks if t['schema'] == 'staging']

# 4. Criar dicionario final
task_dict = {task['task_id']: task for task in tasks_staging}

# 5. Imprimir formatado
print("="*60)
print("TASKS CONFIGURADAS (SCHEMA: STAGING)")
print("="*60)
for task_id, config in task_dict.items():
    print(f"\nTask ID: {task_id}")
    print(f"  Source: {config['source']}")
    print(f"  Target: {config['target']}")
    print(f"  Dependencies: {config['dependencies']}")

print("\n" + "="*60)
print(f"Total: {len(task_dict)} tasks")
print("="*60)

# Output:
# ============================================================
# TASKS CONFIGURADAS (SCHEMA: STAGING)
# ============================================================
#
# Task ID: incremental_e095for
#   Source: e095for
#   Target: stg_fornecedores
#   Dependencies: []
#
# Task ID: incremental_e440nfc
#   Source: e440nfc
#   Target: stg_notas_fiscais
#   Dependencies: ['e095for']
#
# Task ID: full_e640lct
#   Source: e640lct
#   Target: stg_lancamentos
#   Dependencies: ['e440nfc']
#
# ============================================================
# Total: 3 tasks
# ============================================================
```

**Conceitos aplicados:**
- `.to_dict('records')` para conversao
- Iteracao sobre DataFrame
- `pd.isna()` para verificar NaN
- `.split()` e `.strip()` para processar strings
- List comprehension para filtragem
- Dictionary comprehension para estrutura final
- Formatacao de output complexa

</details>

---

## 6.3. Exercícios DAX {#63-exercicios-dax}

---

### 6.3.1. Bloco 1: Medidas Basicas com Filtros

#### Exercício 3.1 - Contagem com Filtro Simples 🟢
**Contexto:** No dashboard de contas a receber, precisamos contar títulos por status.  
**Objetivo:** Praticar COUNT com CALCULATE e filtro direto.

**Dados disponíveis:**
- fat_contas_a_receber (num_titulo, sit_titulo, vlr_original_titulo, dat_vencimento)

**Tarefas:**
1. Criar medida que conta títulos com status "AB" (aberto)
2. Criar medida que conta títulos com status "LQ" (liquidado)

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida 1: Títulos em Aberto
titulos_em_aberto = 
CALCULATE(
    COUNT(fat_contas_a_receber[num_titulo]),
    fat_contas_a_receber[sit_titulo] = "AB"
)

// Medida 2: Títulos Liquidados
titulos_liquidados = 
CALCULATE(
    COUNT(fat_contas_a_receber[num_titulo]),
    fat_contas_a_receber[sit_titulo] = "LQ"
)
```

**Conceitos aplicados:**
- `COUNT()` - Conta linhas nao vazias de uma coluna
- `CALCULATE()` - Modifica o contexto de filtro
- Filtro direto: `tabela[coluna] = "valor"`

</details>

---

#### Exercício 3.2 - Soma com Filtro Simples 🟢
**Contexto:** Além de contar, precisamos somar o valor total dos títulos em aberto.  
**Objetivo:** Praticar SUM com CALCULATE e filtro direto.

**Dados disponíveis:**
- fat_contas_a_receber (num_titulo, sit_titulo, vlr_original_titulo)

**Tarefas:**
1. Criar medida que soma valor de títulos com status "AB"
2. Criar medida que soma valor de títulos com status "LQ"

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida 1: Valor em Aberto
vlr_titulos_em_aberto = 
CALCULATE(
    SUM(fat_contas_a_receber[vlr_original_titulo]),
    fat_contas_a_receber[sit_titulo] = "AB"
)

// Medida 2: Valor Liquidado
vlr_titulos_liquidados = 
CALCULATE(
    SUM(fat_contas_a_receber[vlr_original_titulo]),
    fat_contas_a_receber[sit_titulo] = "LQ"
)
```

**Conceitos aplicados:**
- `SUM()` - Soma valores de uma coluna
- `CALCULATE()` - Aplica filtro a agregacao
- Filtro em contexto de medida

</details>

---

#### Exercício 3.3 - Soma Total com Múltiplos Filtros 🟢
**Contexto:** No dashboard de estoque, precisamos somar gastos por categoria específica.  
**Objetivo:** Praticar SUM com CALCULATE e filtro de texto exato.

**Dados disponíveis:**
- fat_gasto_estoque (vlrmov, grupo_epi, grupo_epi_donos)

**Tarefas:**
1. Criar medida que soma gastos onde grupo_epi_donos = "UNIFORMES"
2. Criar medida que soma gastos onde grupo_epi_donos = "EPI"

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida 1: Total Uniformes
total_uniformes = 
CALCULATE(
    SUM(fat_gasto_estoque[vlrmov]),
    fat_gasto_estoque[grupo_epi_donos] = "UNIFORMES"
)

// Medida 2: Total EPI
total_epi = 
CALCULATE(
    SUM(fat_gasto_estoque[vlrmov]),
    fat_gasto_estoque[grupo_epi_donos] = "EPI"
)
```

**Conceitos aplicados:**
- `SUM()` com filtro categorico
- `CALCULATE()` com filtro de texto
- Agregacao por grupo

</details>

---

### 6.3.2. Bloco 2: Comparacões Temporais

#### Exercício 3.4 - Mês Anterior 🟡
**Contexto:** No dashboard de orcamento TI, comparamos realizado atual vs mês anterior.  
**Objetivo:** Praticar DATEADD para navegar no tempo.

**Dados disponíveis:**
- orcamento_taina (valor_orcado, valor_realizado)
- dim_calendario (data, mes, ano)

**Tarefas:**
1. Criar medida "Realizado" que soma valor_realizado
2. Criar medida "Realizado Mês Anterior" usando DATEADD(-1, MONTH)

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida base
Realizado = SUM(orcamento_taina[valor_realizado])

// Mês anterior
Realizado Mês Anterior = 
CALCULATE(
    [Realizado],
    DATEADD(dim_calendario[data], -1, MONTH)
)
```

**Conceitos aplicados:**
- `DATEADD(coluna_data, quantidade, unidade)` - Navega no tempo
- Unidades: DAY, MONTH, QUARTER, YEAR
- Referência de medida: `[nome_medida]`

</details>

---

#### Exercício 3.5 - Semana Anterior 🟡
**Contexto:** No dashboard de estoque, acompanhamos variacao semanal de gastos.  
**Objetivo:** Praticar DATEADD com dias para comparacao semanal.

**Dados disponíveis:**
- fat_gasto_estoque (vlrmov, data_movimento)
- dim_calendario (data, semana, mes)

**Tarefas:**
1. Criar medida "total" que soma vlrmov
2. Criar medida "total_semana_anterior" usando DATEADD(-7, DAY)

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida base
total = SUM(fat_gasto_estoque[vlrmov])

// Semana anterior (7 dias atras)
total_semana_anterior = 
CALCULATE(
    [total],
    DATEADD(dim_calendario[data], -7, DAY)
)
```

**Conceitos aplicados:**
- `DATEADD()` com DAY para dias específicos
- -7 dias = semana anterior
- Navegacao temporal personalizada

</details>

---

#### Exercício 3.6 - Variacao Percentual 🟡
**Contexto:** Precisamos calcular crescimento percentual entre períodos.  
**Objetivo:** Praticar DIVIDE para evitar erros de divisao por zero.

**Dados disponíveis:**
- Medidas ja criadas: [total], [total_semana_anterior]

**Tarefas:**
1. Criar medida "variacao_absoluta" (diferenca simples)
2. Criar medida "variacao_percentual" usando DIVIDE
3. Formatar como percentual

<details>
<summary>📝 Ver Solucao</summary>

```dax

// Variacao absoluta
variacao_absoluta = [total] - [total_semana_anterior]

// Variacao percentual (segura contra divisao por zero)
variacao_percentual = 
DIVIDE(
    [total] - [total_semana_anterior],
    [total_semana_anterior],
    0  // valor padrao se denominador for zero
)

// Com formatacao
variacao_percentual_formatada = 
FORMAT(
    [variacao_percentual],
    "0.0%"
)

```

**Conceitos aplicados:**
- `DIVIDE(numerador, denominador, valor_alternativo)` - Divisao segura
- Terceiro parâmetro previne erro #DIV/0
- `FORMAT()` para apresentacao

</details>

---

### 6.3.3. Bloco 3: Totalizadores e Contextos

#### Exercício 3.7 - Total Geral (Ignorar Filtros) 🟡
**Contexto:** No dashboard de estoque, queremos calcular % sobre o total ignorando filtros visuais.  
**Objetivo:** Praticar REMOVEFILTERS para criar totalizadores globais.

**Dados disponíveis:**
- fat_gasto_estoque (vlrmov, grupo_epi)

**Tarefas:**
1. Criar medida "total" que respeita filtros
2. Criar medida "total_geral" que ignora filtro de grupo_epi
3. Criar medida "percentual_do_total"

<details>
<summary>📝 Ver Solucao</summary>

```dax

// Total normal (respeita filtros)
total = SUM(fat_gasto_estoque[vlrmov])

// Total geral (ignora filtro de grupo)
total_geral = 
CALCULATE(
    SUM(fat_gasto_estoque[vlrmov]),
    REMOVEFILTERS(fat_gasto_estoque[grupo_epi])
)

// Percentual do total
percentual_do_total = 
DIVIDE(
    [total],
    [total_geral]
)
```

**Conceitos aplicados:**
- `REMOVEFILTERS(tabela[coluna])` - Remove filtro específico
- `REMOVEFILTERS(tabela)` - Remove todos os filtros da tabela
- Total geral vs total filtrado

</details>

---

#### Exercício 3.8 - Subtotal por Categoria 🟡
**Contexto:** Queremos total por categoria mantendo outros filtros ativos (período, filial).  
**Objetivo:** Praticar ALLEXCEPT para remover filtros seletivamente.

**Dados disponíveis:**
- fat_gasto_estoque (vlrmov, grupo_epi, filial, data)
- dim_calendario (data, mes, ano)

**Tarefas:**
1. Criar medida que soma por grupo_epi
2. Ignorar filtro de grupo_epi MAS manter filtros de período e filial

<details>
<summary>📝 Ver Solucao</summary>

```dax

// Total por categoria (mantém filtros de período/filial)
total_categoria = 
CALCULATE(
    SUM(fat_gasto_estoque[vlrmov]),
    ALLEXCEPT(
        fat_gasto_estoque,
        fat_gasto_estoque[filial],
        dim_calendario[data]
    )
)

// Alternativa usando REMOVEFILTERS
total_categoria_v2 = 
CALCULATE(
    SUM(fat_gasto_estoque[vlrmov]),
    REMOVEFILTERS(fat_gasto_estoque[grupo_epi])
)
```

**Conceitos aplicados:**
- `ALLEXCEPT(tabela, coluna1, coluna2, ...)` - Remove todos EXCETO
- Mantém filtros específicos enquanto remove outros
- Útil para subtotais por dimensao

</details>

---

### 6.3.4. Bloco 4: Indicadores e Logica Condicional {#634-bloco-4-indicadores-e-logica-condicional}

#### Exercício 3.9 - Indicador de Meta 🟡
**Contexto:** No dashboard de orcamento, queremos indicar se estamos dentro da meta (±10%).  
**Objetivo:** Praticar IF com logica condicional para criar indicadores.

**Dados disponíveis:**
- orcamento_taina (valor_orcado, valor_realizado)

**Tarefas:**
1. Criar medida "delta_percentual" (realizado vs orcado)
2. Criar medida "status_meta" que retorna "Dentro" ou "Fora"
3. Meta: variacao de até ±10%

<details>
<summary>📝 Ver Solucao</summary>

```dax

// Base: orcado e realizado
Orcado = SUM(orcamento_taina[valor_orcado])
Realizado = SUM(orcamento_taina[valor_realizado])

// Delta percentual
delta_percentual = 
DIVIDE(
    [Realizado] - [Orcado],
    [Orcado],
    0
)

// Status da meta (±10%)
status_meta = 
IF(
    ABS([delta_percentual]) <= 0.10,
    "Dentro da Meta",
    "Fora da Meta"
)

// Versao com cores
status_meta_cor = 
IF(
    ABS([delta_percentual]) <= 0.10,
    "🟢 Dentro",
    "🔴 Fora"
)
```

**Conceitos aplicados:**
- `IF(teste_logico, valor_se_verdadeiro, valor_se_falso)`
- `ABS()` - Valor absoluto (ignora sinal)
- Logica condicional para indicadores

</details>

---

#### Exercício 3.10 - Cor Condicional Baseada em Crescimento 🟡
**Contexto:** No dashboard de estoque, queremos cor verde para crescimento e vermelha para queda.  
**Objetivo:** Praticar IF aninhado para retornar valores condicionais (codigos de cor).

**Dados disponíveis:**
- Medidas ja criadas: [total], [total_semana_anterior]

**Tarefas:**
1. Calcular diferenca entre períodos
2. Retornar codigo de cor: verde (#008000) para crescimento, vermelho (#FF0000) para queda
3. Retornar BLANK() se nao houver dados

<details>
<summary>📝 Ver Solucao</summary>

```dax

// Cor baseada em variacao semanal
Cor_Indicador_Semanal = 
VAR diferenca = [total] - [total_semana_anterior]
RETURN
    IF(
        ISBLANK(diferenca),
        BLANK(),
        IF(
            diferenca >= 0,
            "#008000",  // Verde (crescimento)
            "#FF0000"   // Vermelho (queda)
        )
    )

// Versao com emoji
Indicador_Visual = 
VAR diferenca = [total] - [total_semana_anterior]
RETURN
    IF(
        ISBLANK(diferenca),
        "",
        IF(
            diferenca >= 0,
            "🟢 +" & FORMAT(diferenca, "#,##0"),
            "🔴 " & FORMAT(diferenca, "#,##0")
        )
    )
```

**Conceitos aplicados:**
- `VAR` - Variaveis para armazenar valores intermediarios
- `ISBLANK()` - Verifica se valor é vazio
- IF aninhado para múltiplas condicões
- Codigos HEX para cores no Power BI

</details>

---

### 6.3.5. Bloco 5: Logica Avancada e Iteracao {#635-bloco-5-logica-avancada-e-iteracao}

#### Exercício 3.11 - Consistência Mensal (Meses Dentro da Meta) 🔴
**Contexto:** No dashboard de orcamento TI, queremos saber quantos % dos meses ficaram dentro da meta.  
**Objetivo:** Praticar FILTER + SUMMARIZE + COUNTROWS para analise agregada.

**Dados disponíveis:**
- orcamento_taina (valor_orcado, valor_realizado)
- dim_calendario (data, conc_mes, mes, ano)
- Meta: delta de ±10%

**Tarefas:**
1. Criar tabela resumida por mês com delta %
2. Filtrar apenas meses dentro da meta (±10%)
3. Calcular % de meses dentro da meta

<details>
<summary>📝 Ver Solucao</summary>

```dax

Consistencia Mensal = 
VAR TabelaMensal = 
    SUMMARIZE(
        dim_calendario,
        dim_calendario[conc_mes],
        "DeltaMes", [delta_percentual]
    )
VAR MesesDentroMeta = 
    COUNTROWS(
        FILTER(
            TabelaMensal,
            ABS([DeltaMes]) <= 0.10
        )
    )
VAR TotalMeses = DISTINCTCOUNT(dim_calendario[conc_mes])
RETURN 
    DIVIDE(MesesDentroMeta, TotalMeses, 0)
```

**Conceitos aplicados:**
- `SUMMARIZE(tabela, coluna_grupo, "nome", medida)` - Cria tabela resumida
- `FILTER(tabela, condicao)` - Filtra tabela virtual
- `COUNTROWS()` - Conta linhas de tabela
- `VAR` para organizar logica complexa

</details>

---

#### Exercício 3.12 - Percentual de Ano Decorrido 🔴
**Contexto:** Queremos calcular que % do ano ja passou para comparar orcado vs realizado.  
**Objetivo:** Praticar logica de data complexa com calculo de dias e ano bissexto.

**Dados disponíveis:**
- dim_calendario (data)

**Tarefas:**
1. Calcular quantos dias se passaram desde 01/jan
2. Calcular total de dias do ano (considerar ano bissexto)
3. Retornar % do ano decorrido

<details>
<summary>📝 Ver Solucao</summary>

```dax

% Ano Decorrido = 
VAR DataSelecionada = MAX(dim_calendario[data])
VAR AnoSelecionado = YEAR(DataSelecionada)
VAR InicioAno = DATE(AnoSelecionado, 1, 1)
VAR DiaDoAno = DATEDIFF(InicioAno, DataSelecionada, DAY) + 1
VAR TotalDiasAno = 
    IF(
        MOD(AnoSelecionado, 4) = 0 
        && (MOD(AnoSelecionado, 100) <> 0 || MOD(AnoSelecionado, 400) = 0),
        366,  // Ano bissexto
        365   // Ano normal
    )
RETURN
    DIVIDE(DiaDoAno, TotalDiasAno, 0)
```

**Conceitos aplicados:**
- `YEAR()`, `DATE()` - Funcões de data
- `DATEDIFF(data_inicial, data_final, unidade)` - Diferenca entre datas
- `MOD()` - Resto da divisao (para detectar ano bissexto)
- Logica complexa de ano bissexto

</details>

---

## 6.4. Exercícios Linguagem M (Power Query) {#64-exercicios-linguagem-m-power-query}

---

### 6.4.1. Bloco 1: Transformacões Basicas {#641-bloco-1-transformacoes-basicas}

#### Exercício 4.1 - Unpivot de Colunas Mensais 🟢
**Contexto:** Dados de orcamento chegam com uma coluna para cada mês (jan, fev, mar...).  
**Objetivo:** Entender Table.UnpivotOtherColumns para transformar colunas em linhas.

**Codigo fornecido:**

```m
let
    Fonte = Excel.Workbook(...),
    Planilha = Fonte{[Name="orcamento"]}[Data],
    #"Colunas Removidas" = Table.RemoveColumns(Planilha, {"regra", "amostra"}),
    #"Colunas Nao Dinâmicas" = Table.UnpivotOtherColumns(
        #"Colunas Removidas", 
        {"tipo"}, 
        "Atributo", 
        "Valor"
    )
in
    #"Colunas Nao Dinâmicas"
```

**Perguntas:**
1. O que faz `Table.UnpivotOtherColumns`?
2. Por que {"tipo"} esta no segundo parâmetro?
3. O que significam "Atributo" e "Valor"?

<details>
<summary>📝 Ver Solucao</summary>

**Respostas:**

1. **Table.UnpivotOtherColumns**: Transforma múltiplas colunas em duas colunas (nome + valor)
   - ANTES: `tipo | jan | fev | mar`
   - DEPOIS: `tipo | Atributo | Valor` (3x mais linhas)

2. **{"tipo"}**: Lista de colunas que **NaO** serao dinamizadas
   - Essas colunas permanecem fixas
   - Todas as outras viram linhas

3. **"Atributo" e "Valor"**: Nomes das novas colunas
   - "Atributo": nome da coluna original (jan, fev, mar...)
   - "Valor": conteúdo da célula

**Exemplo visual:**
```
ANTES:
tipo      | jan_orc | jan_real | fev_orc
UNIFORME  | 1000    | 950      | 1100

DEPOIS:
tipo      | Atributo  | Valor
UNIFORME  | jan_orc   | 1000
UNIFORME  | jan_real  | 950
UNIFORME  | fev_orc   | 1100
```

**Conceitos aplicados:**
- Unpivot (wide → long format)
- Colunas fixas vs dinâmicas
- Reestruturacao de dados

</details>

---

#### Exercício 4.2 - Limpeza e Padronizacao de Texto 🟢
**Contexto:** Dados vêm com texto inconsistente (espacos, minúsculas/maiúsculas, caracteres especiais).  
**Objetivo:** Entender Text.Upper, Text.Clean, Text.Trim para padronizar.

**Codigo fornecido:**

```m

let
    Fonte = Excel.Workbook(...),
    #"Texto em Maiúscula" = Table.TransformColumns(
        Fonte,
        {{"tipo", Text.Upper, type text}}
    ),
    #"Texto Limpo" = Table.TransformColumns(
        #"Texto em Maiúscula",
        {{"tipo", Text.Clean, type text}}
    ),
    #"Texto Aparado" = Table.TransformColumns(
        #"Texto Limpo",
        {{"tipo", Text.Trim, type text}}
    )
in
    #"Texto Aparado"
```

**Perguntas:**
1. O que faz `Text.Upper`?
2. Qual a diferenca entre `Text.Clean` e `Text.Trim`?
3. Por que a ordem das transformacões importa?

<details>
<summary>📝 Ver Solucao</summary>

**Respostas:**

1. **Text.Upper**: Converte todo texto para MAIÚSCULAS
   - "uniforme" → "UNIFORME"
   - Útil para padronizacao e comparacões

2. **Diferenca Clean vs Trim**:
   - **Text.Clean**: Remove caracteres nao imprimíveis (quebras de linha, tabs invisíveis)
   - **Text.Trim**: Remove espacos no início e fim
   - Exemplo: " texto  \n " → Clean: " texto   " → Trim: "texto"

3. **Ordem importa**:
   - Primeiro Upper (facilita identificar problemas)
   - Depois Clean (remove sujeira invisível)
   - Por fim Trim (remove espacos extras)

**Exemplo completo:**
```
ANTES:    "  uniforme  \n"
Upper:    "  UNIFORME  \n"
Clean:    "  UNIFORME  "
Trim:     "UNIFORME"
```

**Conceitos aplicados:**
- Transformacao de colunas
- Funcões de texto em M
- Pipeline de limpeza

</details>

---

### 6.4.2. Bloco 2: Transformacões Intermediarias {#642-bloco-2-transformacoes-intermediarias}

#### Exercício 4.3 - Split de Coluna por Delimitador 🟡
**Contexto:** Coluna com dados compostos "orc_jan_2025" precisa ser separada.  
**Objetivo:** Entender Table.SplitColumn e Splitter.SplitTextByDelimiter.

**Codigo fornecido:**

```m

let
    Fonte = ...,
    #"Dividir Coluna por Delimitador" = Table.SplitColumn(
        Fonte, 
        "Atributo", 
        Splitter.SplitTextByDelimiter("_", QuoteStyle.Csv), 
        {"Atributo.1", "Atributo.2", "Atributo.3"}
    ),
    #"Tipo Alterado" = Table.TransformColumnTypes(
        #"Dividir Coluna por Delimitador",
        {{"Atributo.1", type text}, {"Atributo.2", type text}, {"Atributo.3", Int64.Type}}
    ),
    #"Colunas Removidas" = Table.RemoveColumns(#"Tipo Alterado", {"Atributo.3"}),
    #"Colunas Renomeadas" = Table.RenameColumns(
        #"Colunas Removidas",
        {{"Atributo.1", "Orcado_Real"}, {"Atributo.2", "Mes"}}
    )
in
    #"Colunas Renomeadas"
```

**Perguntas:**
1. O que faz `Splitter.SplitTextByDelimiter("_")`?
2. Por que especificar {"Atributo.1", "Atributo.2", "Atributo.3"}?
3. Por que remover "Atributo.3" depois?

<details>
<summary>📝 Ver Solucao</summary>

**Respostas:**

1. **SplitTextByDelimiter("_")**: Divide texto usando "_" como separador
   - "orc_jan_2025" → ["orc", "jan", "2025"]
   - Cada parte vira uma coluna

2. **Lista de nomes**: Define nomes das novas colunas resultantes
   - Sem isso: Power Query gera nomes genéricos (Column1, Column2...)
   - Com isso: Nomes descritivos desde o início

3. **Remover Atributo.3**: Coluna de ano nao era necessaria
   - Split gera 3 colunas, mas so precisavamos de 2
   - Remove coluna extra para simplificar

**Transformacao visual:**

```
ANTES:
Atributo
orc_jan_2025

SPLIT:
Atributo.1 | Atributo.2 | Atributo.3
orc        | jan        | 2025

DEPOIS (remove .3 e renomeia):
Orcado_Real | Mes
orc         | jan
```

**Conceitos aplicados:**
- Split de texto por delimitador
- Nomeacao de colunas resultantes
- Limpeza pos-transformacao

</details>

---

#### Exercício 4.4 - Pivot Apos Unpivot (Reestruturacao) 🟡
**Contexto:** Dados foram desempilhados (unpivot), transformados, e precisam voltar para formato wide.  
**Objetivo:** Entender Table.Pivot para inverter unpivot.

**Codigo fornecido:**

```m

let
    Fonte = ...,
    #"Unpivot" = Table.UnpivotOtherColumns(Fonte, {"tipo"}, "Metrica", "Valor"),
    #"Split Metrica" = Table.SplitColumn(...),  // separa "orc_jan" em "orc" e "jan"
    #"Renomear" = Table.RenameColumns(..., {{"Atributo.1", "Orcado_Real"}}),
    #"Coluna em pivô" = Table.Pivot(
        #"Renomear", 
        List.Distinct(#"Renomear"[Orcado_Real]), 
        "Orcado_Real", 
        "Valor", 
        List.Sum
    )
in
    #"Coluna em pivô"
```

**Perguntas:**
1. O que faz `Table.Pivot`?
2. O que significa `List.Distinct(#"Renomear"[Orcado_Real])`?
3. Por que usar `List.Sum` como último parâmetro?

<details>
<summary>📝 Ver Solucao</summary>

**Respostas:**

1. **Table.Pivot**: Transforma linhas em colunas (oposto de unpivot)
   - ANTES (long): `tipo | Orcado_Real | Valor`
   - DEPOIS (wide): `tipo | orc | real`

2. **List.Distinct**: Lista valores únicos da coluna
   - Ex: ["orc", "real"] se coluna tem "orc", "real", "orc", "real"...
   - Define quais serao os nomes das novas colunas

3. **List.Sum**: Funcao de agregacao para valores duplicados
   - Se houver múltiplas linhas para mesmo tipo+Orcado_Real, soma valores
   - Outras opcões: List.Min, List.Max, List.Average

**Transformacao visual:**
```
ANTES (long):
tipo      | Orcado_Real | Valor
UNIFORME  | orc         | 1000
UNIFORME  | real        | 950
EPI       | orc         | 500
EPI       | real        | 480

DEPOIS (wide - pivot):
tipo      | orc  | real
UNIFORME  | 1000 | 950
EPI       | 500  | 480
```

**Conceitos aplicados:**
- Pivot (long → wide)
- Valores distintos como nomes de colunas
- Agregacao em pivot

</details>

---

### 6.4.3. Bloco 3: Analise de Pipeline Completo {#643-bloco-3-analise-de-pipeline-completo}

#### Exercício 4.5 - Pipeline de Transformacao Completo 🔴
**Contexto:** Analisar codigo M real do orcamento de laboratorio com múltiplas transformacões encadeadas.  
**Objetivo:** Compreender pipeline completo de ETL no Power Query.

**Codigo fornecido (simplificado):**

```m

let
    Fonte = AmazonRedshift.Database("servidor", "database"),
    dbt_marts = Fonte{[Name="dbt_marts"]}[Data],
    fat_orcamento = dbt_marts{[Name="fat_orcamento_laboratorio_custo_fixo"]}[Data],
    
    // 1. Unpivot
    #"Colunas Nao Dinâmicas" = Table.UnpivotOtherColumns(
        fat_orcamento, 
        {"tipo"}, 
        "Atributo", 
        "Valor"
    ),
    
    // 2. Renomear
    #"Colunas Renomeadas" = Table.RenameColumns(
        #"Colunas Nao Dinâmicas",
        {{"Atributo", "Mês"}, {"tipo", "TIPO"}}
    ),
    
    // 3. Split
    #"Dividir Coluna por Delimitador" = Table.SplitColumn(
        #"Colunas Renomeadas", 
        "Mês", 
        Splitter.SplitTextByDelimiter("_", QuoteStyle.Csv), 
        {"Mês.1", "Mês.2", "Mês.3"}
    ),
    
    // 4. Remover coluna ano
    #"Colunas Removidas" = Table.RemoveColumns(
        #"Dividir Coluna por Delimitador",
        {"Mês.3"}
    ),
    
    // 5. Renomear split
    #"Colunas Renomeadas1" = Table.RenameColumns(
        #"Colunas Removidas",
        {{"Mês.1", "Orcado Real"}, {"Mês.2", "Mês"}}
    ),
    
    // 6. Pivot
    #"Coluna em pivô" = Table.Pivot(
        #"Colunas Renomeadas1", 
        List.Distinct(#"Colunas Renomeadas1"[#"Orcado Real"]), 
        "Orcado Real", 
        "Valor", 
        List.Sum
    ),
    
    // 7. Renomear colunas finais
    #"Colunas Renomeadas2" = Table.RenameColumns(
        #"Coluna em pivô",
        {{"orc", "valor_orc2025"}, {"real", "valor_real2025"}}
    ),
    
    // 8. Substituir valores específicos
    #"Valor Substituído" = Table.ReplaceValue(
        #"Colunas Renomeadas2",
        5161.73,
        0,
        Replacer.ReplaceValue,
        {"valor_real2025"}
    ),
    
    // 9. Converter zeros em null
    #"Valor Substituído2" = Table.ReplaceValue(
        #"Valor Substituído",
        0,
        null,
        Replacer.ReplaceValue,
        {"valor_orc2025"}
    ),
    
    // 10. Padronizar texto
    #"Texto em Maiúscula" = Table.TransformColumns(
        #"Valor Substituído2",
        {{"TIPO", Text.Upper, type text}}
    ),
    
    // 11. Limpar caracteres especiais
    #"Valor Substituído4" = Table.ReplaceValue(
        #"Texto em Maiúscula",
        "Ô",
        "O",
        Replacer.ReplaceText,
        {"TIPO"}
    ),
    
    // 12. Limpeza final
    #"Texto Limpo" = Table.TransformColumns(
        #"Valor Substituído4",
        {{"TIPO", Text.Clean, type text}}
    )
in
    #"Texto Limpo"
```

**Perguntas:**
1. Qual a estrutura inicial vs final dos dados?
2. Por que unpivot → split → pivot (ida e volta)?
3. Quais sao as 3 fases principais do pipeline?
4. Por que tantos steps de substituicao de valores?

<details>
<summary>📝 Ver Solucao</summary>

**Respostas:**

**1. Estrutura Inicial vs Final:**

INICIAL (wide):
```
tipo      | orc_jan_2025 | real_jan_2025 | orc_fev_2025 | real_fev_2025
INSUMOS   | 1000         | 950           | 1100         | 1050
```

FINAL (semi-wide):
```
TIPO      | Mês | valor_orc2025 | valor_real2025
INSUMOS   | jan | 1000          | 950
INSUMOS   | fev | 1100          | 1050
```

**2. Por que Unpivot → Split → Pivot?**

- **Unpivot**: Traz todas as colunas mensais para linhas (facilita split)
- **Split**: Separa "orc_jan_2025" em ["orc", "jan", "2025"]
- **Pivot**: Volta "orc" e "real" para colunas (formato final desejado)

É mais facil processar texto quando esta em linhas!

**3. Três Fases Principais:**

**FASE 1 - Reestruturacao (steps 1-6):**
- Unpivot → Split → Pivot
- Objetivo: Formato long com colunas orc/real separadas

**FASE 2 - Limpeza de Dados (steps 7-9):**
- Substituir valores específicos
- Converter zeros em null
- Objetivo: Dados limpos e corretos

**FASE 3 - Padronizacao de Texto (steps 10-12):**
- Maiúsculas
- Remover acentos
- Limpar caracteres especiais
- Objetivo: Texto consistente para analise

**4. Por que Tantas Substituicões?**

- **Valor específico (5161.73 → 0)**: Valor incorreto conhecido no source
- **Zeros → null**: Distinguir "sem dados" de "valor zero real"
- **Caracteres especiais**: Garantir compatibilidade e padronizacao

**Pipeline Visual:**
```
1. CONECTAR    → Redshift (raw)
2. NAVEGAR     → dbt_marts.fat_orcamento
3. UNPIVOT     → Meses em linhas
4. SPLIT       → "orc_jan_2025" → ["orc","jan","2025"]
5. LIMPAR      → Remove coluna ano
6. PIVOT       → orc/real voltam para colunas
7. CORRIGIR    → Valores errados
8. PADRONIZAR  → Texto limpo e maiúsculo
9. ENTREGAR    → Tabela final
```

**Conceitos aplicados:**
- Pipeline ETL completo
- Transformacões encadeadas
- Logica de reestruturacao (unpivot-pivot)
- Qualidade de dados (limpeza + padronizacao)
- Navegacao em objetos aninhados

</details>

---

## Contato

**Joao Lima**  
Contabilidade  
Sítio Nossa Senhora Aparecida, Zona Rural  
CEP 14160-970 - Caixa Postal 167  
Sertaozinho - Sao Paulo - Brasil  
Fone: (16) 2105-5300<br>
GitHub: [joaofdl9](https://github.com/joaofdl9)

---

*Documento gerado como parte do processo de handover - Janeiro 2026*
