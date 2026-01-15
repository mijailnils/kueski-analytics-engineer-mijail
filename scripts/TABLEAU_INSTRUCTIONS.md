# Ì≥ä INSTRUCCIONES PARA TABLEAU

## ÌæØ ARCHIVOS DISPONIBLES

### Excel Files (Usar estos en Tableau)
1. `data/exports/fct_loan_financials.xlsx` - Tabla principal (9,396 pr√©stamos)
2. `data/exports/fct_cohort_performance.xlsx` - Performance por cohorte
3. `data/exports/fct_portfolio_pnl.xlsx` - P&L por segmento
4. `data/exports/fct_vintage_curves.xlsx` - Curvas de delinquency

### SQL Queries
`queries_para_tableau.sql` - 12 queries listas para usar

## Ì≥ã DASHBOARDS RECOMENDADOS

### Dashboard 1: Executive Summary
**Queries:** 1, 2
**Visualizaciones:**
- 4 KPI cards: Total Loans, Revenue, LTV/CAC, Delinquency
- Bar chart horizontal: LTV/CAC por segmento
- 2 Pie charts: Portfolio mix (volumen y revenue)
- Line chart: Delinquency evolution

### Dashboard 2: Segment Deep Dive
**Queries:** 4, 5, 7
**Visualizaciones:**
- Heatmap: Vintage √ó Segment performance
- Bar chart: Performance por Risk Band
- Box plot: CAC distribution

### Dashboard 3: Vintage Curves
**Query:** 3
**Visualizaciones:**
- Line chart: Delinquency por mes de maduraci√≥n
- Multiple lines: Una l√≠nea por vintage (Ene, Feb, Mar)

### Dashboard 4: Customer Behavior
**Query:** 6, 10
**Visualizaciones:**
- Side-by-side bars: Primer vs Recurrente
- Cohort retention chart

## ÔøΩÔøΩ C√ìMO CONECTAR TABLEAU

### Opci√≥n 1: Excel (Recomendado)
1. Abrir Tableau
2. Connect to Data ‚Üí Excel
3. Navegar a `data/exports/`
4. Seleccionar archivo
5. Crear relaciones entre tablas si es necesario

### Opci√≥n 2: SQL Queries
1. Copiar query de `queries_para_tableau.sql`
2. En Tableau: New Custom SQL
3. Pegar query
4. Ejecutar

## Ì≥à M√âTRICAS CLAVE

**LTV/CAC por Segmento:**
- High Risk: **10.37x** ‚≠ê
- Medium Risk: **5.85x** ‚úÖ
- Low Risk: **2.47x** ‚ùå

**Hallazgos:**
- 45% de clientes son recurrentes
- Recurrentes tienen -43% delinquency
- CAC solo se asigna en pr√©stamo #1
