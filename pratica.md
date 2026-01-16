# 6. Pratica

## 6.1. Exercicios SQL Analiticos


Exercicios propostos para desenvolver habilidade essencial de Analise de Dados: realizar consultas SQL analiticas que permitam responder perguntas de negocio de forma eficiente e precisa.

### Estrutura dos Exercicios

| Bloco | Exercicios | Conceito | Nivel |
|-------|------------|----------|-------|
| 1 | EX1-5 | SELECT + WHERE | ⭐ |
| 2 | EX6-10 | JOINs | ⭐⭐ |
| 3 | EX11-15 | Agregacoes | ⭐⭐ |
| 4 | EX16-20 | CTEs | ⭐⭐⭐ |
| 5 | EX21-25 | Window Functions | ⭐⭐⭐ |
| 6 | EX26-30 | UNION + Subconsultas + DML | ⭐⭐⭐ |

### Tabelas Principais

`E440NFC`, `E440IPC`, `E095FOR`, `E440ISC`, `E660INC`, `E075PRO`, `E070FIL`

### Aplicacoes

Contabil | Fiscal | Compras | Financeiro | Controladoria

---

### 6.1.1. Basico (SELECT + WHERE)

#### EX1: Consulta de Lancamentos Contabeis com Filtro por Conta Especifica

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

**Retornar:** Codigo Empresa, Codigo Filial, Número da NF, Codigo do Fornecedor, Codigo do Produto, Quantidade Recebida, Preco Unitario, Valor Liquido e Data de Geracao.

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

**Critérios:** Mes de Marco de 2025, ordenado por valor bruto decrescente.

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

### 6.1.2. JOINs

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

#### EX7: Itens de Produto com Informacoes Detalhadas de Produto e Fornecedor

**Objetivo:** Analisar itens de compra relacionando produtos, fornecedores e notas fiscais

**Retornar:** Número NF, Nome Fornecedor, Codigo Produto, Descricao Produto, Quantidade Recebida, Preco Unitario, Valor Total do Item, Data de Entrada.

**Critérios:** Mes de Fevereiro 2025, produtos com valor total superior a R$ 1.000,00.

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

**Objetivo:** Relacionar dados fiscais de entrada com informacoes organizacionais

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

### 6.1.3. Agregacoes

#### EX11: Totalizacao de Compras por Fornecedor no Trimestre

**Objetivo:** Agregar valores de compras por fornecedor para analise de volume de aquisicoes

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

**Objetivo:** Consolidar valores de créditos tributarios (PIS e COFINS) por filial e mes

**Retornar:** Codigo Filial, Nome Filial, Mes/Ano, Quantidade de Itens, Total Base PIS, Total PIS Recuperavel, Total Base COFINS, Total COFINS Recuperavel.

**Critérios:** Primeiro semestre de 2025, agrupar por filial e mes, apenas itens com crédito.

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

**Retornar:** Codigo Produto, Descricao Produto, Quantidade Total Comprada, Quantidade de Fornecedores, Preco Minimo, Preco Maximo, Preco Médio, Valor Total Gasto.

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

**Objetivo:** Sumarizar gastos com servicos por centro de custo incluindo retencoes tributarias

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

### 6.1.4. CTEs

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

**Objetivo:** Criar CTE para agregar impostos por mes e depois calcular variacoes

**Retornar:** Mes, Ano, Total Base ICMS, Total ICMS, Total IPI, Total PIS, Total COFINS, Carga Tributaria Efetiva (%).

**Critérios:** Primeiro semestre de 2025, agrupar por mes.

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

#### EX20: CTE Complexa - Analise de Variacao Mes a Mes com Múltiplas Dimensoes

**Objetivo:** Usar CTEs para calcular totais mensais e depois comparar com mes anterior

**Retornar:** Mes Atual, Ano, Valor Compras Mes Atual, Valor Mes Anterior, Variacao Absoluta, Variacao Percentual, Quantidade Fornecedores Ativos, Ticket Médio.

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

### 6.1.5. Window Functions

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

#### EX24: Funcoes LAG e LEAD - Comparacao com Mes Anterior e Posterior

**Objetivo:** Calcular valor mensal de compras e comparar com meses adjacentes

**Retornar:** Mes, Ano, Valor do Mes, Valor Mes Anterior, Valor Mes Seguinte, Variacao vs Mes Anterior (valor absoluto).

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

**Objetivo:** Calcular valor acumulado mes a mes e percentual sobre o total do periodo

**Retornar:** Mes, Ano, Valor Mensal, Acumulado até o Mes, Percentual sobre Total do Semestre, Total do Semestre.

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

### 6.1.6. UNION + Subconsultas + DML

#### EX26: UNION Basico - Listar Todos os Itens de Compra

**Objetivo:** Combinar produtos e servicos em uma única lista usando UNION

**Retornar:** Tipo (Produto ou Servico), Codigo do Item, Número da NF, Valor.

**Critérios:** Mes de Janeiro 2025, ordenar por tipo e valor.

---

#### EX27: Subconsulta Simples - Notas Acima da Média

**Objetivo:** Usar subconsulta para filtrar notas fiscais com valor acima da média do periodo

**Retornar:** Codigo Filial, Número NF, Codigo Fornecedor, Valor da Nota, Média do Periodo.

**Critérios:** Primeiro trimestre de 2025.

---

#### EX28: EXISTS Basico - Fornecedores com Compras no Periodo

**Objetivo:** Identificar fornecedores que tiveram pelo menos uma compra no periodo usando EXISTS

**Retornar:** Codigo Fornecedor, Nome Fornecedor, Cidade, UF.

**Critérios:** Fornecedores com compras em Janeiro 2025.

---

#### EX29: Funcoes de String e Data Basicas - Formatacao de Fornecedores

**Objetivo:** Aplicar funcoes de string e data para formatar e filtrar informacoes

**Retornar:** Codigo Fornecedor, Nome em Maiúsculo, Primeiros 20 caracteres do Nome, Data da Última Compra, Dias desde a Última Compra.

**Critérios:** Fornecedores que compraram nos últimos 90 dias, nome contém "COMERCIO".

---

#### EX30: INSERT SELECT Basico - Tabela Resumo de Fornecedores

**Objetivo:** Criar tabela temporaria e inserir dados resumidos usando INSERT SELECT

**Retornar:** Tabela com totais por fornecedor para analise.

**Critérios:** Janeiro 2025, criar resumo simples.

---

### 6.2. Python

---

#### 6.2.1. Sintaxe Básica

> ℹ️ **Setup:** Apenas Python 3.8+. Nenhuma instalação necessária.

---

##### Exercício 1.1 - Variáveis e Tipos 🟢

**Exemplo**
```python
# Variáveis de controle usadas em extract_upload_full_manual()
total_rows = 0              # int
first_chunk = True          # bool
temp_csv_path = None        # NoneType
chunk_size = 10_000         # int (underscores para legibilidade)
tabela = "e660nfc"          # str
```

---

**Prever**
```python
rowcount = 1500
s3_bytes = 0
status = "success"
processado = False

print(type(rowcount))
print(type(processado))
print(type(status))
```

<details>
<summary>📝 Ver Solução</summary>

```
<class 'int'>
<class 'bool'>
<class 'str'>
```
**Conceitos:** `type()` retorna a classe do valor.
</details>

---

**Corrigir**
```python
tabela_origem = e660nfc
total_rows = "1500"
total_rows += 100
```

<details>
<summary>📝 Ver Solução</summary>

```python
tabela_origem = "e660nfc"       # String precisa de aspas
total_rows = 1500               # Sem aspas para somar
total_rows += 100
```
**Conceitos:** Strings requerem aspas; não pode somar str + int.
</details>

---

**Modificar**
```python
payload = {"rowcount": "2500", "s3_bytes": "102400"}
# TODO: Converter ambos para int
```

<details>
<summary>📝 Ver Solução</summary>

```python
payload["rowcount"] = int(payload["rowcount"])
payload["s3_bytes"] = int(payload["s3_bytes"])
```
**Conceitos:** `int()` converte string numérica.
</details>

---

**Criar**

Crie variáveis para retorno de extração: tabela ("e640lct"), rows_processed (15000), success (True), error_message (None). Imprima cada valor e seu tipo.

<details>
<summary>📝 Ver Solução</summary>

```python
tabela = "e640lct"
rows_processed = 15000
success = True
error_message = None

print(f"{tabela} - {type(tabela)}")
print(f"{rows_processed} - {type(rows_processed)}")
print(f"{success} - {type(success)}")
print(f"{error_message} - {type(error_message)}")
```
**Conceitos:** Tipos básicos: str, int, bool, NoneType.
</details>

---

##### Exercício 1.2 - Strings 🟢

**Exemplo**
```python
tabela = "e660nfc"
sql = f"SELECT * FROM dbo.{tabela}"         # "SELECT * FROM dbo.e660nfc"
s3_key = f"comercial/{tabela}_temp.csv.gz"  # "comercial/e660nfc_temp.csv.gz"
tabela[:4]                                   # "e660"
tabela.startswith("e66")                     # True
```

---

**Prever**
```python
tabela_destino = "airbyte_raw.e660nfc"
print(f"s3://bm-airflow/comercial/temp.csv.gz")
print(tabela_destino.split("."))
print(tabela_destino.replace("raw", "staging"))
```

<details>
<summary>📝 Ver Solução</summary>

```
s3://bm-airflow/comercial/temp.csv.gz
['airbyte_raw', 'e660nfc']
airbyte_staging.e660nfc
```
**Conceitos:** f-strings, `.split()`, `.replace()`.
</details>

---

**Corrigir**
```python
tabela = "e640lct"
schema = "dbt_marts"
sql = f"SELECT * FROM schema.tabela"
```

<details>
<summary>📝 Ver Solução</summary>

```python
sql = f"SELECT * FROM {schema}.{tabela}"  # Variáveis precisam de {}
```
**Conceitos:** Em f-string, variáveis precisam de `{}`.
</details>

---

**Modificar**
```python
tabela_destino = "airbyte_raw.e660nfc"
# TODO: Extrair apenas o nome da tabela (após o ponto) -> "e660nfc"
```

<details>
<summary>📝 Ver Solução</summary>

```python
nome_tabela = tabela_destino.split(".")[-1]
```
**Conceitos:** `.split()` retorna lista; índice -1 pega último.
</details>

---

**Criar**

Dado: tabela="e660nfc", schema="airbyte_raw", linhas=5000. Construa tabela_destino, s3_key e log formatado com milhar.

<details>
<summary>📝 Ver Solução</summary>

```python
tabela = "e660nfc"
schema = "airbyte_raw"
linhas = 5000

tabela_destino = f"{schema}.{tabela}"
s3_key = f"comercial/{tabela}_batch_nrt_temp.csv.gz"
log = f"[INFO] Tabela {tabela}: {linhas:,} linhas"
```
**Conceitos:** f-strings com `:,` formatam números.
</details>

---

##### Exercício 1.3 - Listas 🟢

**Exemplo**
```python
tabelas = ["e660nfc", "e660inc", "e640lct"]
tabelas[0]              # "e660nfc"
tabelas[-1]             # "e640lct"
tabelas.append("e075for")
", ".join(tabelas)      # "e660nfc, e660inc, e640lct, e075for"
```

---

**Prever**
```python
created_objects = []
created_objects.append({"s3_key": "part-001.csv.gz", "bytes": 1024})
created_objects.append({"s3_key": "part-002.csv.gz", "bytes": 2048})
print(len(created_objects))
print(created_objects[-1]["bytes"])
```

<details>
<summary>📝 Ver Solução</summary>

```
2
2048
```
**Conceitos:** Lista de dicts; acesso encadeado.
</details>

---

**Corrigir**
```python
tabelas = ["e660nfc", "e640lct"]
tabelas.append["e075for"]
colunas = ["col1", "col2"]
sql_cols = colunas.join(", ")
```

<details>
<summary>📝 Ver Solução</summary>

```python
tabelas.append("e075for")       # () não []
sql_cols = ", ".join(colunas)   # String.join(lista)
```
**Conceitos:** Métodos usam `()`; `.join()` é método de string.
</details>

---

**Modificar**
```python
cols = ["cod_emp", "num_nfc", "dat_ent", "vlr_tot"]
# TODO: Transformar em "cod_emp, num_nfc, dat_ent, vlr_tot"
```

<details>
<summary>📝 Ver Solução</summary>

```python
select_cols = ", ".join(cols)
```
**Conceitos:** `", ".join(lista)` concatena com separador.
</details>

---

**Criar**

Simule created_objects: lista vazia, adicione 3 dicts com s3_key e bytes, calcule total_bytes.

<details>
<summary>📝 Ver Solução</summary>

```python
created_objects = []
created_objects.append({"s3_key": "part-001.csv.gz", "bytes": 1024})
created_objects.append({"s3_key": "part-002.csv.gz", "bytes": 2048})
created_objects.append({"s3_key": "part-003.csv.gz", "bytes": 512})

total_bytes = 0
for obj in created_objects:
    total_bytes += obj["bytes"]
print(f"Total: {total_bytes:,}")  # 3,584
```
**Conceitos:** Lista de dicts, loop para somar.
</details>

---

##### Exercício 1.4 - Dicionários 🟡

**Exemplo**
```python
payload = {"status": "success", "rowcount": 15000, "s3_bytes": 102400}
payload["rowcount"]             # 15000
payload.get("error", None)      # None (não existe)
payload["elapsed_sec"] = 45.2   # Adiciona chave
```

---

**Prever**
```python
partition_info = {"partition_column": "USU_DTHROP", "window_days": 7}
print(partition_info["window_days"])
print(partition_info.get("schema", "public"))
print(list(partition_info.keys()))
```

<details>
<summary>📝 Ver Solução</summary>

```
7
public
['partition_column', 'window_days']
```
**Conceitos:** `.get(chave, default)` evita KeyError.
</details>

---

**Corrigir**
```python
resultado = {"status": "success", "rows": 1000}
print(resultado[status])
print(resultado.get("erro", "Sem erro")
```

<details>
<summary>📝 Ver Solução</summary>

```python
print(resultado["status"])      # Chave é string
print(resultado.get("erro", "Sem erro"))  # Fechar parêntese
```
**Conceitos:** Chaves são strings; parênteses balanceados.
</details>

---

**Modificar**
```python
payload = {"rowcount": 5000, "s3_bytes": 20480}
# TODO: Adicionar status="success" e window_start="2025-01-08"
```

<details>
<summary>📝 Ver Solução</summary>

```python
payload["status"] = "success"
payload["window_start"] = "2025-01-08"
```
**Conceitos:** Adicionar chave com atribuição.
</details>

---

**Criar**

Crie função get_partition_info(tabela) com dict interno para e660nfc e e640lct. Retorne partition_column e window_days, ou erro se não existe.

<details>
<summary>📝 Ver Solução</summary>

```python
def get_partition_info(tabela):
    configs = {
        "e660nfc": {"partition_column": "USU_DTHROP", "window_days": 7},
        "e640lct": {"partition_column": "DAT_LCT", "window_days": 30}
    }
    return configs.get(tabela, {"error": "Tabela não configurada"})
```
**Conceitos:** Dict de dicts; `.get()` com default.
</details>

---

##### Exercício 1.5 - Condicionais e Loops 🟡

**Exemplo**
```python
while True:
    rows = cursor.fetchmany(CHUNK_SIZE)
    if not rows:
        break
    total_rows += len(rows)

if metodo == "load_sql":
    sql_script = load_sql(f"select_{tabela}")
elif metodo == "csv":
    payload = extract_upload(...)
else:
    raise ValueError(f"Método desconhecido: {metodo}")
```

---

**Prever**
```python
chunks = [[1,2,3], [4,5,6], [7,8], []]
total = 0
for chunk in chunks:
    if not chunk:
        break
    total += len(chunk)
print(f"Total: {total}")
```

<details>
<summary>📝 Ver Solução</summary>

```
Total: 8
```
**Conceitos:** `if not lista` é True quando vazia; `break` sai do loop.
</details>

---

**Corrigir**
```python
if status == "success"
    print("OK")
for tabela in ["e660nfc", "e640lct"]
    print(tabela)
```

<details>
<summary>📝 Ver Solução</summary>

```python
if status == "success":     # Faltava :
    print("OK")
for tabela in ["e660nfc", "e640lct"]:  # Faltava :
    print(tabela)
```
**Conceitos:** Estruturas de controle terminam com `:`.
</details>

---

**Modificar**
```python
tabelas = ["e660nfc", "e660inc", "e640lct", "e075for"]
for tabela in tabelas:
    print(f"Processando: {tabela}")
# TODO: Processar apenas tabelas que começam com "e66"
```

<details>
<summary>📝 Ver Solução</summary>

```python
for tabela in tabelas:
    if tabela.startswith("e66"):
        print(f"Processando: {tabela}")
```
**Conceitos:** Filtro com `if` dentro do loop.
</details>

---

**Criar**

Simule loop de chunks: chunks = [[dados], [dados], []], pare quando vazio, conte total processado.

<details>
<summary>📝 Ver Solução</summary>

```python
chunks = [["a","b","c"], ["d","e"], []]
total = 0
chunk_count = 0
for chunk in chunks:
    if not chunk:
        print("Fim dos dados")
        break
    chunk_count += 1
    total += len(chunk)
    print(f"Chunk {chunk_count}: {len(chunk)} registros")
print(f"Total: {total}")
```
**Conceitos:** Loop com break, contadores.
</details>

---

#### 6.2.2. Recursos da Linguagem

##### Exercício 2.1 - Funções e Erros 🟡

**Exemplo**
```python
def extract_upload(tabela, s3_bucket, s3_key, parameter=""):
    conn = None
    try:
        conn = hook.get_conn()
        return {"status": "success", "rows": total_rows}
    except Exception as e:
        raise
    finally:
        if conn: conn.close()
```

---

**Prever**
```python
def calc_chunk_size(num_colunas):
    chunk = min(10_000_000 // num_colunas, 1_000_000)
    return max(chunk, 50_000)

print(calc_chunk_size(50))
print(calc_chunk_size(500))
```

<details>
<summary>📝 Ver Solução</summary>

```
200000
50000
```
**Conceitos:** `//` divisão inteira; `min/max` para limitar.
</details>

---

**Corrigir**
```python
def compute_window_start(window_days)
    from datetime import date, timedelta
    start = date.today() - timedelta(days=window_days)
    start.isoformat()
```

<details>
<summary>📝 Ver Solução</summary>

```python
def compute_window_start(window_days):   # Faltava :
    from datetime import date, timedelta
    start = date.today() - timedelta(days=window_days)
    return start.isoformat()              # Faltava return
```
**Conceitos:** Função precisa de `:` e `return`.
</details>

---

**Modificar**
```python
def delete_and_copy(tabela, payload):
    rowcount = payload["rowcount"]
    s3_bytes = payload["s3_bytes"]
    return {"status": "success"}
# TODO: Adicionar validação que levanta ValueError se s3_bytes < 64
```

<details>
<summary>📝 Ver Solução</summary>

```python
def delete_and_copy(tabela, payload, s3_min_bytes=64):
    s3_bytes = payload["s3_bytes"]
    if s3_bytes < s3_min_bytes:
        raise ValueError(f"Arquivo muito pequeno: {s3_bytes} bytes")
    return {"status": "success"}
```
**Conceitos:** `raise` levanta exceção; parâmetro com default.
</details>

---

**Criar**

Crie função truncate_and_insert(tabela, rows_data) com try/except/finally que simula operação e retorna dict com status e rows.

<details>
<summary>📝 Ver Solução</summary>

```python
def truncate_and_insert(tabela_destino, rows_data):
    try:
        print(f"TRUNCATE TABLE {tabela_destino}")
        print(f"INSERT {len(rows_data)} rows")
        return {"status": "success", "rows": len(rows_data)}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        print("Conexão fechada")
```
**Conceitos:** try/except/finally; retorno de dict.
</details>

---

##### Exercício 2.2 - Comprehensions 🟡

**Exemplo**
```python
cols = [desc[0] for desc in cursor.description]
tabelas_nf = [t for t in tabelas if t.startswith("e66")]
tamanhos = {t: len(t) for t in tabelas}
total_bytes = sum(obj["bytes"] for obj in created_objects)
```

---

**Prever**
```python
cursor_description = [("cod_emp",), ("num_nfc",), ("vlr_tot",)]
cols = [desc[0] for desc in cursor_description]
cols_upper = [c.upper() for c in cols]
print(cols)
print(cols_upper)
```

<details>
<summary>📝 Ver Solução</summary>

```
['cod_emp', 'num_nfc', 'vlr_tot']
['COD_EMP', 'NUM_NFC', 'VLR_TOT']
```
**Conceitos:** Comprehension extrai elementos; encadeamento.
</details>

---

**Corrigir**
```python
tabelas = ["e660nfc", "e640lct", "e075for"]
nf_tables = [t for t in tabelas if t.startswith("e66"]
sizes = {t, len(t) for t in tabelas}
```

<details>
<summary>📝 Ver Solução</summary>

```python
nf_tables = [t for t in tabelas if t.startswith("e66")]  # Faltava )
sizes = {t: len(t) for t in tabelas}  # Dict usa :, não ,
```
**Conceitos:** Parênteses balanceados; dict usa `:`.
</details>

---

**Modificar**
```python
created_objects = [
    {"s3_key": "part-001.csv.gz", "bytes": 1024},
    {"s3_key": "part-002.csv.gz", "bytes": 2048},
]
# Loop atual
total_bytes = 0
for obj in created_objects:
    total_bytes += obj["bytes"]
# TODO: Converter para sum() com generator
```

<details>
<summary>📝 Ver Solução</summary>

```python
total_bytes = sum(obj["bytes"] for obj in created_objects)
```
**Conceitos:** `sum()` com generator expression.
</details>

---

**Criar**

Dado cursor_description = [("cod_emp", "varchar"), ("num_nfc", "int")], crie: lista de nomes, dict {nome: tipo}, string para SELECT.

<details>
<summary>📝 Ver Solução</summary>

```python
cursor_description = [("cod_emp", "varchar"), ("num_nfc", "int"), ("vlr_tot", "decimal")]
nomes = [desc[0] for desc in cursor_description]
tipos = {desc[0]: desc[1] for desc in cursor_description}
select_cols = ", ".join(nomes)
```
**Conceitos:** List/dict comprehension; `.join()`.
</details>

---

##### Exercício 2.3 - Tempo e Logging 🟡

**Exemplo**
```python
import time
from datetime import datetime, timedelta

t0 = time.time()
# processamento
duracao = time.time() - t0
print(f"✓ {tabela} OK - {rowcount:,} rows em {duracao:.1f}s")

window_start = datetime.now().date() - timedelta(days=7)
```

---

**Prever**
```python
import time
inicio = time.time()
time.sleep(0.5)
duracao = time.time() - inicio
print(f"Duração: {duracao:.2f}s")
print(type(duracao))
```

<details>
<summary>📝 Ver Solução</summary>

```
Duração: 0.50s
<class 'float'>
```
**Conceitos:** `time.time()` retorna float; `:.2f` formata.
</details>

---

**Corrigir**
```python
import time
from datetime import datetime, timedelta
inicio = time.time
duracao = time.time - inicio
janela = datetime.now().date - timedelta(days=7)
```

<details>
<summary>📝 Ver Solução</summary>

```python
inicio = time.time()            # Faltava ()
duracao = time.time() - inicio
janela = datetime.now().date() - timedelta(days=7)  # date()
```
**Conceitos:** Funções precisam de `()`.
</details>

---

**Modificar**
```python
import time
inicio = time.time()
rows = 15000
duracao = time.time() - inicio
print(f"Duração: {duracao:.1f}s")
# TODO: Adicionar velocidade (rows/s) formatada
```

<details>
<summary>📝 Ver Solução</summary>

```python
velocidade = rows / duracao if duracao > 0 else 0
print(f"✓ {rows:,} linhas em {duracao:.1f}s | {velocidade:,.0f} rows/s")
```
**Conceitos:** Cálculo de velocidade; formatação.
</details>

---

**Criar**

Crie função que mede tempo de operação simulada, calcula velocidade e retorna dict com rows, duration_seconds, rows_per_second.

<details>
<summary>📝 Ver Solução</summary>

```python
import time

def transform_and_load(tabela, rows_count):
    start = time.time()
    time.sleep(0.1)  # Simula
    duration = time.time() - start
    rows_per_sec = round(rows_count / duration) if duration > 0 else 0
    print(f"✓ {rows_count:,} linhas em {tabela} ({duration:.1f}s | {rows_per_sec:,} rows/s)")
    return {"rows": rows_count, "duration_seconds": round(duration, 2), "rows_per_second": rows_per_sec}
```
**Conceitos:** Medição de tempo; métricas; retorno estruturado.
</details>

---

#### 6.2.3. Pandas

> ⚙️ **Setup:** `pip install pandas`

---

##### Exercício 3.1 - Criar e Explorar 🟢

**Exemplo**
```python
import pandas as pd
df = pd.read_csv("schema.csv", sep=";", encoding="utf-8")
df.columns = df.columns.str.strip()
df.shape        # (linhas, colunas)
df.head(3)      # Primeiras 3
```

---

**Prever**
```python
import pandas as pd
df = pd.DataFrame({
    "table_name": ["e660nfc", "e660nfc", "e640lct", "e640lct", "e640lct"],
    "column_name": ["cod_emp", "num_nfc", "cod_emp", "num_lct", "vlr_deb"]
})
print(df.shape)
print(len(df[df["table_name"] == "e640lct"]))
```

<details>
<summary>📝 Ver Solução</summary>

```
(5, 2)
3
```
**Conceitos:** `.shape` retorna tupla; filtro retorna subset.
</details>

---

**Corrigir**
```python
import pandas as pd
df = pd.dataframe({"col": [1, 2, 3]})
print(df.Shape)
colunas = df.columns.tolist
```

<details>
<summary>📝 Ver Solução</summary>

```python
df = pd.DataFrame({"col": [1, 2, 3]})  # DataFrame maiúsculo
print(df.shape)                         # minúsculo
colunas = df.columns.tolist()           # ()
```
**Conceitos:** Case-sensitive; `.tolist()` é método.
</details>

---

**Modificar**
```python
df = pd.read_csv("schema.csv")
# TODO: Adicionar sep=";", encoding="utf-8" e limpar espaços das colunas
```

<details>
<summary>📝 Ver Solução</summary>

```python
df = pd.read_csv("schema.csv", sep=";", encoding="utf-8")
df.columns = df.columns.str.strip()
```
**Conceitos:** Parâmetros de leitura; `.str.strip()`.
</details>

---

**Criar**

Crie DataFrame simulando schema.csv (table_name, column_name, select). Imprima shape e quantidade de colunas de e640lct.

<details>
<summary>📝 Ver Solução</summary>

```python
import pandas as pd
df = pd.DataFrame({
    "table_name": ["e660nfc", "e660nfc", "e640lct", "e640lct"],
    "column_name": ["cod_emp", "num_nfc", "cod_emp", "num_lct"],
    "select": ["cod_emp", "num_nfc", "cod_emp", "num_lct"]
})
print(f"Shape: {df.shape}")
print(f"Colunas e640lct: {len(df[df['table_name'] == 'e640lct'])}")
```
**Conceitos:** Criação de DataFrame; filtro e contagem.
</details>

---

##### Exercício 3.2 - Filtrar e Selecionar 🟡

**Exemplo**
```python
cols = df[df["table_name"] == tabela]["select"].tolist()
df_filtered = df[(df["table_name"] == "e660nfc") & (df["order"] <= 3)]
```

---

**Prever**
```python
import pandas as pd
df = pd.DataFrame({
    "tabela": ["e660nfc", "e660nfc", "e640lct"],
    "coluna": ["cod_emp", "vlr_tot", "cod_emp"]
})
result = df[df["tabela"] == "e660nfc"]["coluna"].tolist()
print(result)
print(", ".join(result))
```

<details>
<summary>📝 Ver Solução</summary>

```
['cod_emp', 'vlr_tot']
cod_emp, vlr_tot
```
**Conceitos:** Filtro + seleção + `.tolist()` + `.join()`.
</details>

---

**Corrigir**
```python
df_nf = df[df["table_name"].startswith("e66")]
df_filtered = df[df["tipo"] == "varchar" and df["order"] == 1]
```

<details>
<summary>📝 Ver Solução</summary>

```python
df_nf = df[df["table_name"].str.startswith("e66")]  # .str
df_filtered = df[(df["tipo"] == "varchar") & (df["order"] == 1)]  # & com ()
```
**Conceitos:** `.str.startswith()` para Series; `&` com parênteses.
</details>

---

**Modificar**
```python
tabela = "e660nfc"
cols = df[df["table_name"] == tabela]["select"].tolist()
# TODO: Modificar para pegar todas tabelas que começam com "e66"
```

<details>
<summary>📝 Ver Solução</summary>

```python
cols_nf = df[df["table_name"].str.startswith("e66")]["select"].tolist()
```
**Conceitos:** `.str.startswith()` filtra por prefixo.
</details>

---

**Criar**

Crie função get_select_sql(tabela) que filtra DataFrame e retorna SQL formatado.

<details>
<summary>📝 Ver Solução</summary>

```python
import pandas as pd
df = pd.DataFrame({
    "table_name": ["e660nfc", "e660nfc", "e640lct"],
    "select": ["cod_emp", "vlr_tot", "cod_emp"]
})

def get_select_sql(tabela):
    cols = df[df["table_name"] == tabela]["select"].tolist()
    return f"SELECT {', '.join(cols)} FROM dbo.{tabela}"

print(get_select_sql("e660nfc"))
```
**Conceitos:** Filtro, `.tolist()`, `.join()`, f-string.
</details>

---

##### Exercício 3.3 - Merge e Transformar 🟡

**Exemplo**
```python
df = df.merge(df_dominios, on="tabela_fato", how="left")
df["dominio"] = df["dominio"].fillna("outros")
df["tabela"] = df["tabela"].str.replace(r"\s+", "_", regex=True)
for _, row in df.iterrows():
    print(row["tabela"])
```

---

**Prever**
```python
import pandas as pd
df1 = pd.DataFrame({"tabela": ["e660nfc", "e640lct"], "tipo": ["nf", "contabil"]})
df2 = pd.DataFrame({"tabela": ["e660nfc"], "dominio": ["fiscal"]})
merged = df1.merge(df2, on="tabela", how="left")
print(merged["dominio"].isna().sum())
```

<details>
<summary>📝 Ver Solução</summary>

```
1
```
**Conceitos:** Merge left mantém todas de df1; sem match vira NaN.
</details>

---

**Corrigir**
```python
merged = df1.merge(df2, on="tabela")  # Perde linhas!
merged["info"] = merged["info"].fillna["desconhecido"]
```

<details>
<summary>📝 Ver Solução</summary>

```python
merged = df1.merge(df2, on="tabela", how="left")  # how="left"
merged["info"] = merged["info"].fillna("desconhecido")  # ()
```
**Conceitos:** `how="left"` mantém linhas; `fillna()` usa `()`.
</details>

---

**Modificar**
```python
df = pd.DataFrame({
    "tabela_fato": ["fat vendas", "fat compras"],
    "tabela_origem": ["e660 nfc", "e660 inc"]
})
# TODO: Substituir espaços por _ em ambas colunas
```

<details>
<summary>📝 Ver Solução</summary>

```python
df["tabela_fato"] = df["tabela_fato"].str.replace(r"\s+", "_", regex=True)
df["tabela_origem"] = df["tabela_origem"].str.replace(r"\s+", "_", regex=True)
```
**Conceitos:** `.str.replace()` com regex.
</details>

---

**Criar**

Simule DAG: crie df_config e df_dominios, faça merge left, preencha NaN, itere com iterrows.

<details>
<summary>📝 Ver Solução</summary>

```python
import pandas as pd
df_config = pd.DataFrame({
    "tabela_origem": ["e660nfc", "e640lct"],
    "tabela_fato": ["fat_vendas", "fat_contabil"]
})
df_dominios = pd.DataFrame({
    "tabela_fato": ["fat_vendas"],
    "dominio": ["fiscal"]
})
df = df_config.merge(df_dominios, on="tabela_fato", how="left")
df["dominio"] = df["dominio"].fillna("outros")
for _, row in df.iterrows():
    print(f"Task: {row['tabela_origem']} -> {row['tabela_fato']} [{row['dominio']}]")
```
**Conceitos:** Merge, fillna, iterrows - padrão DAG NRT.
</details>

---

#### 6.2.4. Arquivos e Dados

##### Exercício 4.1 - Caminhos e Diretórios 🟡

**Exemplo**
```python
import os
from pathlib import Path

filename = os.path.join(current_dir, "include", "seed", "schema.csv")
BASE_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = BASE_DIR / "include" / "seed" / "config.csv"
bucket = os.getenv("S3_BUCKET", "bm-airflow")
```

---

**Prever**
```python
from pathlib import Path
base = Path("/home/airflow/dags")
csv_path = base / "include" / "seed" / "schema.csv"
print(csv_path)
print(csv_path.name)
```

<details>
<summary>📝 Ver Solução</summary>

```
/home/airflow/dags/include/seed/schema.csv
schema.csv
```
**Conceitos:** `/` concatena; `.name` extrai nome do arquivo.
</details>

---

**Corrigir**
```python
path = Path("include" / "seed" / "file.csv")
bucket = os.getenv("BUCKET")
s3_key = f"{bucket}/data.csv"  # None/data.csv se não existe
```

<details>
<summary>📝 Ver Solução</summary>

```python
path = Path("include") / "seed" / "file.csv"  # Path() no início
bucket = os.getenv("BUCKET", "default-bucket")  # default
```
**Conceitos:** `Path()` no início; `os.getenv(key, default)`.
</details>

---

**Modificar**
```python
csv_path = "/home/airflow/dags/include/seed/schema.csv"
# TODO: Refatorar usando os.path.join()
```

<details>
<summary>📝 Ver Solução</summary>

```python
import os
csv_path = os.path.join("/home/airflow/dags", "include", "seed", "schema.csv")
```
**Conceitos:** `os.path.join()` é cross-platform.
</details>

---

**Criar**

Crie função get_csv_path(source) que retorna path para cadastro_tabelas_{source}.csv usando pathlib.

<details>
<summary>📝 Ver Solução</summary>

```python
from pathlib import Path

def get_csv_path(source):
    base_dir = Path("/home/airflow/dags")
    return base_dir / "include" / "seed" / f"cadastro_tabelas_{source.lower()}.csv"
```
**Conceitos:** pathlib, f-string, `.lower()`.
</details>

---

##### Exercício 4.2 - CSV e JSON 🟡

**Exemplo**
```python
import csv
import json

with open("dados.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["col1", "col2"])
    writer.writerows([(1, "a"), (2, "b")])

with open("config.csv", "r") as f:
    reader = csv.DictReader(f, delimiter=";")
    for row in reader:
        print(row["tabela"])

json.dump(data, f, ensure_ascii=False, indent=2)
data = json.load(f)
```

---

**Prever**
```python
import csv
from io import StringIO
csv_content = "nome;valor\nproduto1;100\nproduto2;200"
reader = csv.DictReader(StringIO(csv_content), delimiter=";")
rows = list(reader)
print(len(rows))
print(rows[0]["nome"])
```

<details>
<summary>📝 Ver Solução</summary>

```
2
produto1
```
**Conceitos:** DictReader transforma CSV em lista de dicts.
</details>

---

**Corrigir**
```python
import csv, json
with open("dados.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow["col1", "col2"]
with open("dados.json", "r") as f:
    data = json.loads(f)
```

<details>
<summary>📝 Ver Solução</summary>

```python
writer.writerow(["col1", "col2"])  # ()
data = json.load(f)  # load, não loads
```
**Conceitos:** Métodos usam `()`; `load` para arquivo, `loads` para string.
</details>

---

**Modificar**
```python
import json
data = [{"id": 1}, {"id": 2}]
with open("output.json", "w") as f:
    json.dump(data, f)
# TODO: Adicionar formatação legível e suporte a acentos
```

<details>
<summary>📝 Ver Solução</summary>

```python
with open("output.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
```
**Conceitos:** `ensure_ascii=False` mantém acentos; `indent` formata.
</details>

---

**Criar**

Fluxo API: crie lista de dicts, salve JSON, leia de volta, converta para CSV com DictWriter.

<details>
<summary>📝 Ver Solução</summary>

```python
import csv, json

data = [{"id": 1, "nome": "a"}, {"id": 2, "nome": "b"}]

with open("api.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

with open("api.json", "r") as f:
    data = json.load(f)

with open("output.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "nome"])
    writer.writeheader()
    writer.writerows(data)
```
**Conceitos:** Fluxo JSON→CSV.
</details>

---

##### Exercício 4.3 - Compressão (gzip) 🟡

**Exemplo**
```python
import gzip
import csv
import tempfile

tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz")
with gzip.open(tmp.name, "wt", newline="", encoding="utf-8", compresslevel=4) as gz:
    writer = csv.writer(gz)
    writer.writerow(["col1", "col2"])
    writer.writerows(rows)
```

---

**Prever**
```python
import gzip
with gzip.open("teste.csv.gz", "wt", encoding="utf-8") as f:
    f.write("col1,col2\na,1\nb,2\n")
with gzip.open("teste.csv.gz", "rt", encoding="utf-8") as f:
    content = f.read()
print(content.count("\n"))
```

<details>
<summary>📝 Ver Solução</summary>

```
3
```
**Conceitos:** `wt`/`rt` = write/read text; gzip transparente.
</details>

---

**Corrigir**
```python
import gzip
with gzip.open("dados.gz", "w") as f:
    f.write("texto")  # Erro: esperava bytes
with gzip.open("dados.gz", "r") as f:
    texto = f.read()  # Retorna bytes
```

<details>
<summary>📝 Ver Solução</summary>

```python
with gzip.open("dados.gz", "wt", encoding="utf-8") as f:  # wt
    f.write("texto")
with gzip.open("dados.gz", "rt", encoding="utf-8") as f:  # rt
    texto = f.read()
```
**Conceitos:** `wt`/`rt` para texto; `w`/`r` para bytes.
</details>

---

**Modificar**
```python
import csv
rows = [("a", 1), ("b", 2)]
with open("dados.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(rows)
# TODO: Modificar para CSV comprimido com compresslevel=4
```

<details>
<summary>📝 Ver Solução</summary>

```python
import gzip
with gzip.open("dados.csv.gz", "wt", newline="", encoding="utf-8", compresslevel=4) as f:
    writer = csv.writer(f)
    writer.writerows(rows)
```
**Conceitos:** Trocar `open` por `gzip.open` com `"wt"`.
</details>

---

**Criar**

Simule extract: processe chunks até vazio, escreva CSV.gz com header só no primeiro, use tempfile.

<details>
<summary>📝 Ver Solução</summary>

```python
import gzip, csv, tempfile

chunks = [[("1","a"), ("2","b")], [("3","c")], []]
tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv.gz")
total = 0

with gzip.open(tmp.name, "wt", newline="", encoding="utf-8", compresslevel=4) as gz:
    writer = csv.writer(gz)
    first = True
    for chunk in chunks:
        if not chunk:
            break
        if first:
            writer.writerow(["id", "nome"])
            first = False
        writer.writerows(chunk)
        total += len(chunk)

print(f"Total: {total} em {tmp.name}")
```
**Conceitos:** Chunks, header condicional, tempfile, gzip+csv.
</details>

---

##### Exercício 4.4 - Banco e S3 🔴

> ⚙️ **Setup:** Mocks para exercícios
```python
class CursorMock:
    def __init__(self, data, cols):
        self.data = data
        self.pos = 0
        self.description = [(c,) for c in cols]
    def execute(self, sql): print(f"[SQL] {sql[:40]}...")
    def fetchmany(self, size):
        chunk = self.data[self.pos:self.pos+size]
        self.pos += size
        return chunk
    def close(self): pass

class S3Mock:
    def upload_file(self, local, bucket, key):
        print(f"[S3] Upload: {local} -> s3://{bucket}/{key}")
    def delete_object(self, Bucket, Key):
        print(f"[S3] Delete: s3://{Bucket}/{Key}")
```

---

**Prever**
```python
data = [(1,"a"), (2,"b"), (3,"c"), (4,"d"), (5,"e")]
cursor = CursorMock(data, ["id", "nome"])
cursor.execute("SELECT * FROM t")
print(cursor.fetchmany(2))
print(cursor.fetchmany(2))
print(cursor.fetchmany(2))
```

<details>
<summary>📝 Ver Solução</summary>

```
[SQL] SELECT * FROM t...
[(1, 'a'), (2, 'b')]
[(3, 'c'), (4, 'd')]
[(5, 'e')]
```
**Conceitos:** fetchmany retorna chunks; último pode ser menor.
</details>

---

**Corrigir**
```python
import boto3
s3 = boto3.client("s3")
s3.upload_file("bm-airflow", "comercial/data.csv", "/tmp/data.csv")
s3.delete_object("bm-airflow", "comercial/data.csv")
```

<details>
<summary>📝 Ver Solução</summary>

```python
s3.upload_file("/tmp/data.csv", "bm-airflow", "comercial/data.csv")  # local, bucket, key
s3.delete_object(Bucket="bm-airflow", Key="comercial/data.csv")  # kwargs
```
**Conceitos:** Ordem: local, bucket, key; kwargs para delete.
</details>

---

**Modificar**
```python
def extract_data(cursor, chunk_size=1000):
    total = 0
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        total += len(rows)
    return total
# TODO: Adicionar print de progresso: "Chunk #N: X linhas | Total: Y"
```

<details>
<summary>📝 Ver Solução</summary>

```python
def extract_data(cursor, chunk_size=1000):
    total = 0
    chunk_count = 0
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        chunk_count += 1
        total += len(rows)
        print(f"Chunk #{chunk_count}: {len(rows)} linhas | Total: {total}")
    return total
```
**Conceitos:** Contador; logging de progresso.
</details>

---

**Criar**

Crie extract_to_s3(cursor, s3, bucket, key, chunk_size): extraia colunas, processe chunks, simule upload, retorne dict.

<details>
<summary>📝 Ver Solução</summary>

```python
def extract_to_s3(cursor, s3, bucket, s3_key, chunk_size=1000):
    cols = [desc[0] for desc in cursor.description]
    total_rows = 0
    chunk_count = 0
    
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        chunk_count += 1
        total_rows += len(rows)
        print(f"Chunk #{chunk_count}: {len(rows)} | Total: {total_rows}")
    
    s3.upload_file("/tmp/temp.csv.gz", bucket, s3_key)
    cursor.close()
    
    return {"status": "success", "rowcount": total_rows, "chunks": chunk_count}

# Teste
data = [(i, f"item_{i}") for i in range(7)]
cursor = CursorMock(data, ["id", "nome"])
result = extract_to_s3(cursor, S3Mock(), "bm-airflow", "test.csv.gz", chunk_size=3)
```
**Conceitos:** Padrão completo de extração.
</details>

---

### Resumo

| Seção | Exercícios |
|-------|------------|
| 6.2.1 Sintaxe | 20 |
| 6.2.2 Recursos | 12 |
| 6.2.3 Pandas | 12 |
| 6.2.4 Arquivos | 16 |
| **Total** | **60** |

**Cobertura:** get_el_tasks.py, get_schema.py, get_t_tasks.py, get_table_metadata.py, nrt_autogen_callables.py, job_controladoria_nrt.py


---

## 6.3. Exercicios DAX

---

### 6.3.1. Medidas Basicas com Filtros

#### Exercicio 3.1 - Contagem com Filtro Simples 🟢
**Contexto:** No dashboard de contas a receber, precisamos contar titulos por status.  
**Objetivo:** Praticar COUNT com CALCULATE e filtro direto.

**Dados disponiveis:**
- fat_contas_a_receber (num_titulo, sit_titulo, vlr_original_titulo, dat_vencimento)

**Tarefas:**
1. Criar medida que conta titulos com status "AB" (aberto)
2. Criar medida que conta titulos com status "LQ" (liquidado)

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida 1: Titulos em Aberto
titulos_em_aberto = 
CALCULATE(
    COUNT(fat_contas_a_receber[num_titulo]),
    fat_contas_a_receber[sit_titulo] = "AB"
)

// Medida 2: Titulos Liquidados
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

#### Exercicio 3.2 - Soma com Filtro Simples 🟢
**Contexto:** Além de contar, precisamos somar o valor total dos titulos em aberto.  
**Objetivo:** Praticar SUM com CALCULATE e filtro direto.

**Dados disponiveis:**
- fat_contas_a_receber (num_titulo, sit_titulo, vlr_original_titulo)

**Tarefas:**
1. Criar medida que soma valor de titulos com status "AB"
2. Criar medida que soma valor de titulos com status "LQ"

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

#### Exercicio 3.3 - Soma Total com Múltiplos Filtros 🟢
**Contexto:** No dashboard de estoque, precisamos somar gastos por categoria especifica.  
**Objetivo:** Praticar SUM com CALCULATE e filtro de texto exato.

**Dados disponiveis:**
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

### 6.3.2. Comparacoes Temporais

#### Exercicio 3.4 - Mes Anterior 🟡
**Contexto:** No dashboard de orcamento TI, comparamos realizado atual vs mes anterior.  
**Objetivo:** Praticar DATEADD para navegar no tempo.

**Dados disponiveis:**
- orcamento_taina (valor_orcado, valor_realizado)
- dim_calendario (data, mes, ano)

**Tarefas:**
1. Criar medida "Realizado" que soma valor_realizado
2. Criar medida "Realizado Mes Anterior" usando DATEADD(-1, MONTH)

<details>
<summary>📝 Ver Solucao</summary>

```dax
// Medida base
Realizado = SUM(orcamento_taina[valor_realizado])

// Mes anterior
Realizado Mes Anterior = 
CALCULATE(
    [Realizado],
    DATEADD(dim_calendario[data], -1, MONTH)
)
```

**Conceitos aplicados:**
- `DATEADD(coluna_data, quantidade, unidade)` - Navega no tempo
- Unidades: DAY, MONTH, QUARTER, YEAR
- Referencia de medida: `[nome_medida]`

</details>

---

#### Exercicio 3.5 - Semana Anterior 🟡
**Contexto:** No dashboard de estoque, acompanhamos variacao semanal de gastos.  
**Objetivo:** Praticar DATEADD com dias para comparacao semanal.

**Dados disponiveis:**
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
- `DATEADD()` com DAY para dias especificos
- -7 dias = semana anterior
- Navegacao temporal personalizada

</details>

---

#### Exercicio 3.6 - Variacao Percentual 🟡
**Contexto:** Precisamos calcular crescimento percentual entre periodos.  
**Objetivo:** Praticar DIVIDE para evitar erros de divisao por zero.

**Dados disponiveis:**
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
- Terceiro parametro previne erro #DIV/0
- `FORMAT()` para apresentacao

</details>

---

### 6.3.3. Totalizadores e Contextos

#### Exercicio 3.7 - Total Geral (Ignorar Filtros) 🟡
**Contexto:** No dashboard de estoque, queremos calcular % sobre o total ignorando filtros visuais.  
**Objetivo:** Praticar REMOVEFILTERS para criar totalizadores globais.

**Dados disponiveis:**
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
- `REMOVEFILTERS(tabela[coluna])` - Remove filtro especifico
- `REMOVEFILTERS(tabela)` - Remove todos os filtros da tabela
- Total geral vs total filtrado

</details>

---

#### Exercicio 3.8 - Subtotal por Categoria 🟡
**Contexto:** Queremos total por categoria mantendo outros filtros ativos (periodo, filial).  
**Objetivo:** Praticar ALLEXCEPT para remover filtros seletivamente.

**Dados disponiveis:**
- fat_gasto_estoque (vlrmov, grupo_epi, filial, data)
- dim_calendario (data, mes, ano)

**Tarefas:**
1. Criar medida que soma por grupo_epi
2. Ignorar filtro de grupo_epi MAS manter filtros de periodo e filial

<details>
<summary>📝 Ver Solucao</summary>

```dax

// Total por categoria (mantém filtros de periodo/filial)
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
- Mantém filtros especificos enquanto remove outros
- Útil para subtotais por dimensao

</details>

---

### 6.3.4. Indicadores e Logica Condicional

#### Exercicio 3.9 - Indicador de Meta 🟡
**Contexto:** No dashboard de orcamento, queremos indicar se estamos dentro da meta (±10%).  
**Objetivo:** Praticar IF com logica condicional para criar indicadores.

**Dados disponiveis:**
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

#### Exercicio 3.10 - Cor Condicional Baseada em Crescimento 🟡
**Contexto:** No dashboard de estoque, queremos cor verde para crescimento e vermelha para queda.  
**Objetivo:** Praticar IF aninhado para retornar valores condicionais (codigos de cor).

**Dados disponiveis:**
- Medidas ja criadas: [total], [total_semana_anterior]

**Tarefas:**
1. Calcular diferenca entre periodos
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
- IF aninhado para múltiplas condicoes
- Codigos HEX para cores no Power BI

</details>

---

### 6.3.5. Logica Avancada e Iteracao

#### Exercicio 3.11 - Consistencia Mensal (Meses Dentro da Meta) 🔴
**Contexto:** No dashboard de orcamento TI, queremos saber quantos % dos meses ficaram dentro da meta.  
**Objetivo:** Praticar FILTER + SUMMARIZE + COUNTROWS para analise agregada.

**Dados disponiveis:**
- orcamento_taina (valor_orcado, valor_realizado)
- dim_calendario (data, conc_mes, mes, ano)
- Meta: delta de ±10%

**Tarefas:**
1. Criar tabela resumida por mes com delta %
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

#### Exercicio 3.12 - Percentual de Ano Decorrido 🔴
**Contexto:** Queremos calcular que % do ano ja passou para comparar orcado vs realizado.  
**Objetivo:** Praticar logica de data complexa com calculo de dias e ano bissexto.

**Dados disponiveis:**
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
- `YEAR()`, `DATE()` - Funcoes de data
- `DATEDIFF(data_inicial, data_final, unidade)` - Diferenca entre datas
- `MOD()` - Resto da divisao (para detectar ano bissexto)
- Logica complexa de ano bissexto

</details>

---

## 6.4. Exercicios Linguagem M (Power Query)

---

### 6.4.1. Transformacoes Basicas

#### Exercicio 4.1 - Unpivot de Colunas Mensais 🟢
**Contexto:** Dados de orcamento chegam com uma coluna para cada mes (jan, fev, mar...).  
**Objetivo:** Entender Table.UnpivotOtherColumns para transformar colunas em linhas.

**Codigo fornecido:**

```m
let
    Fonte = Excel.Workbook(...),
    Planilha = Fonte{[Name="orcamento"]}[Data],
    #"Colunas Removidas" = Table.RemoveColumns(Planilha, {"regra", "amostra"}),
    #"Colunas Nao Dinamicas" = Table.UnpivotOtherColumns(
        #"Colunas Removidas", 
        {"tipo"}, 
        "Atributo", 
        "Valor"
    )
in
    #"Colunas Nao Dinamicas"
```

**Perguntas:**
1. O que faz `Table.UnpivotOtherColumns`?
2. Por que {"tipo"} esta no segundo parametro?
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
- Colunas fixas vs dinamicas
- Reestruturacao de dados

</details>

---

#### Exercicio 4.2 - Limpeza e Padronizacao de Texto 🟢
**Contexto:** Dados vem com texto inconsistente (espacos, minúsculas/maiúsculas, caracteres especiais).  
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
3. Por que a ordem das transformacoes importa?

<details>
<summary>📝 Ver Solucao</summary>

**Respostas:**

1. **Text.Upper**: Converte todo texto para MAIÚSCULAS
   - "uniforme" → "UNIFORME"
   - Útil para padronizacao e comparacoes

2. **Diferenca Clean vs Trim**:
   - **Text.Clean**: Remove caracteres nao imprimiveis (quebras de linha, tabs invisiveis)
   - **Text.Trim**: Remove espacos no inicio e fim
   - Exemplo: " texto  \n " → Clean: " texto   " → Trim: "texto"

3. **Ordem importa**:
   - Primeiro Upper (facilita identificar problemas)
   - Depois Clean (remove sujeira invisivel)
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
- Funcoes de texto em M
- Pipeline de limpeza

</details>

---

### 6.4.2. Transformacoes Intermediarias

#### Exercicio 4.3 - Split de Coluna por Delimitador 🟡
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
   - Com isso: Nomes descritivos desde o inicio

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

#### Exercicio 4.4 - Pivot Apos Unpivot (Reestruturacao) 🟡
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
3. Por que usar `List.Sum` como último parametro?

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
   - Outras opcoes: List.Min, List.Max, List.Average

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

### 6.4.3. Analise de Pipeline Completo

#### Exercicio 4.5 - Pipeline de Transformacao Completo 🔴
**Contexto:** Analisar codigo M real do orcamento de laboratorio com múltiplas transformacoes encadeadas.  
**Objetivo:** Compreender pipeline completo de ETL no Power Query.

**Codigo fornecido (simplificado):**

```m

let
    Fonte = AmazonRedshift.Database("servidor", "database"),
    dbt_marts = Fonte{[Name="dbt_marts"]}[Data],
    fat_orcamento = dbt_marts{[Name="fat_orcamento_laboratorio_custo_fixo"]}[Data],
    
    // 1. Unpivot
    #"Colunas Nao Dinamicas" = Table.UnpivotOtherColumns(
        fat_orcamento, 
        {"tipo"}, 
        "Atributo", 
        "Valor"
    ),
    
    // 2. Renomear
    #"Colunas Renomeadas" = Table.RenameColumns(
        #"Colunas Nao Dinamicas",
        {{"Atributo", "Mes"}, {"tipo", "TIPO"}}
    ),
    
    // 3. Split
    #"Dividir Coluna por Delimitador" = Table.SplitColumn(
        #"Colunas Renomeadas", 
        "Mes", 
        Splitter.SplitTextByDelimiter("_", QuoteStyle.Csv), 
        {"Mes.1", "Mes.2", "Mes.3"}
    ),
    
    // 4. Remover coluna ano
    #"Colunas Removidas" = Table.RemoveColumns(
        #"Dividir Coluna por Delimitador",
        {"Mes.3"}
    ),
    
    // 5. Renomear split
    #"Colunas Renomeadas1" = Table.RenameColumns(
        #"Colunas Removidas",
        {{"Mes.1", "Orcado Real"}, {"Mes.2", "Mes"}}
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
    
    // 8. Substituir valores especificos
    #"Valor Substituido" = Table.ReplaceValue(
        #"Colunas Renomeadas2",
        5161.73,
        0,
        Replacer.ReplaceValue,
        {"valor_real2025"}
    ),
    
    // 9. Converter zeros em null
    #"Valor Substituido2" = Table.ReplaceValue(
        #"Valor Substituido",
        0,
        null,
        Replacer.ReplaceValue,
        {"valor_orc2025"}
    ),
    
    // 10. Padronizar texto
    #"Texto em Maiúscula" = Table.TransformColumns(
        #"Valor Substituido2",
        {{"TIPO", Text.Upper, type text}}
    ),
    
    // 11. Limpar caracteres especiais
    #"Valor Substituido4" = Table.ReplaceValue(
        #"Texto em Maiúscula",
        "Ô",
        "O",
        Replacer.ReplaceText,
        {"TIPO"}
    ),
    
    // 12. Limpeza final
    #"Texto Limpo" = Table.TransformColumns(
        #"Valor Substituido4",
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
TIPO      | Mes | valor_orc2025 | valor_real2025
INSUMOS   | jan | 1000          | 950
INSUMOS   | fev | 1100          | 1050
```

**2. Por que Unpivot → Split → Pivot?**

- **Unpivot**: Traz todas as colunas mensais para linhas (facilita split)
- **Split**: Separa "orc_jan_2025" em ["orc", "jan", "2025"]
- **Pivot**: Volta "orc" e "real" para colunas (formato final desejado)

É mais facil processar texto quando esta em linhas!

**3. Tres Fases Principais:**

**FASE 1 - Reestruturacao (steps 1-6):**
- Unpivot → Split → Pivot
- Objetivo: Formato long com colunas orc/real separadas

**FASE 2 - Limpeza de Dados (steps 7-9):**
- Substituir valores especificos
- Converter zeros em null
- Objetivo: Dados limpos e corretos

**FASE 3 - Padronizacao de Texto (steps 10-12):**
- Maiúsculas
- Remover acentos
- Limpar caracteres especiais
- Objetivo: Texto consistente para analise

**4. Por que Tantas Substituicoes?**

- **Valor especifico (5161.73 → 0)**: Valor incorreto conhecido no source
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
- Transformacoes encadeadas
- Logica de reestruturacao (unpivot-pivot)
- Qualidade de dados (limpeza + padronizacao)
- Navegacao em objetos aninhados

</details>
