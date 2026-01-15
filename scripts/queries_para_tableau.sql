-- ============================================================================
-- QUERIES PARA TABLEAU - KUESKI PORTFOLIO Q1 2025
-- ============================================================================
-- Usar estos queries para conectar Tableau a los Excel exports
-- O ejecutarlos directamente en DuckDB si Tableau tiene conector
-- ============================================================================

-- ============================================================================
-- QUERY 1: EXECUTIVE SUMMARY - KPIs PRINCIPALES
-- ============================================================================
-- Usar: Dashboard principal, tarjetas de KPIs

WITH customer_lifetime AS (
    SELECT 
        l.user_id,
        MAX(l.risk_segment) as risk_segment,
        SUM(l.revenue_total) as lifetime_revenue,
        SUM(l.cac) as total_cac_paid,
        COUNT(DISTINCT l.loan_id) as total_loans
    FROM main.fct_loan_financials l
    WHERE l.risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    GROUP BY l.user_id
)
SELECT 
    risk_segment,
    COUNT(DISTINCT user_id) as customers,
    SUM(total_loans) as loans,
    ROUND(SUM(lifetime_revenue), 0) as total_revenue,
    ROUND(SUM(total_cac_paid), 0) as total_cac,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac_paid), 0), 2) as ltv_to_cac,
    ROUND(AVG(total_loans), 1) as avg_loans_per_customer,
    ROUND(AVG(lifetime_revenue), 0) as avg_ltv,
    ROUND(SUM(lifetime_revenue) / SUM(total_loans), 0) as avg_revenue_per_loan
FROM customer_lifetime
GROUP BY risk_segment
ORDER BY total_revenue DESC;


-- ============================================================================
-- QUERY 2: PORTFOLIO MIX - VOLUMEN Y REVENUE
-- ============================================================================
-- Usar: Pie charts, donut charts

SELECT 
    risk_segment,
    COUNT(*) as total_loans,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct_loans,
    ROUND(SUM(revenue_total), 0) as total_revenue,
    ROUND(SUM(revenue_total) * 100.0 / SUM(SUM(revenue_total)) OVER (), 1) as pct_revenue,
    ROUND(AVG(revenue_total), 0) as avg_revenue_per_loan
FROM main.fct_loan_financials
WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
GROUP BY risk_segment
ORDER BY total_revenue DESC;


-- ============================================================================
-- QUERY 3: VINTAGE CURVES - DELINQUENCY EVOLUTION
-- ============================================================================
-- Usar: Line chart con múltiples líneas (una por vintage)

SELECT 
    vintage_month,
    months_on_book,
    loans_in_cohort,
    delinquent_count,
    ROUND(delinquency_rate * 100, 2) as delinquency_pct,
    ROUND(total_capital_outstanding, 0) as capital_outstanding
FROM main.fct_vintage_curves
WHERE vintage_month IN ('2025-01-01', '2025-02-01', '2025-03-01')
ORDER BY vintage_month, months_on_book;


-- ============================================================================
-- QUERY 4: VINTAGE × SEGMENT PERFORMANCE
-- ============================================================================
-- Usar: Heatmap, pivot table

WITH customer_lifetime AS (
    SELECT 
        l.user_id,
        DATE_TRUNC('month', l.vintage_month) as vintage,
        MAX(l.risk_segment) as risk_segment,
        SUM(l.revenue_total) as lifetime_revenue,
        SUM(l.cac) as total_cac
    FROM main.fct_loan_financials l
    WHERE l.risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    GROUP BY l.user_id, vintage
)
SELECT 
    STRFTIME(vintage, '%Y-%m') as vintage_month,
    risk_segment,
    COUNT(DISTINCT user_id) as customers,
    ROUND(SUM(lifetime_revenue), 0) as revenue,
    ROUND(SUM(total_cac), 0) as cac,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac), 0), 2) as ltv_cac
FROM customer_lifetime
GROUP BY vintage, risk_segment
ORDER BY vintage, risk_segment;


-- ============================================================================
-- QUERY 5: RISK BAND GRANULAR ANALYSIS
-- ============================================================================
-- Usar: Bar charts comparativos

WITH customer_lifetime AS (
    SELECT 
        user_id,
        MAX(risk_band_production) as risk_band,
        MAX(risk_segment) as risk_segment,
        SUM(revenue_total) as lifetime_revenue,
        SUM(cac) as total_cac,
        AVG(CASE WHEN is_delinquent = 1 THEN 1.0 ELSE 0.0 END) as delinq_rate
    FROM main.fct_loan_financials
    WHERE risk_band_production IN ('1', '2', '3', '4.1', '4.2', '5')
    GROUP BY user_id
)
SELECT 
    risk_band,
    risk_segment,
    COUNT(*) as customers,
    ROUND(AVG(lifetime_revenue), 0) as avg_ltv,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac), 0), 2) as ltv_cac,
    ROUND(AVG(delinq_rate) * 100, 1) as avg_delinq_pct
FROM customer_lifetime
GROUP BY risk_band, risk_segment
ORDER BY 
    CASE 
        WHEN risk_band = '1' THEN 1
        WHEN risk_band = '2' THEN 2
        WHEN risk_band = '3' THEN 3
        WHEN risk_band = '4.1' THEN 4
        WHEN risk_band = '4.2' THEN 5
        WHEN risk_band = '5' THEN 6
    END;


-- ============================================================================
-- QUERY 6: PRIMER PRESTAMO VS RECURRENTE
-- ============================================================================
-- Usar: Side-by-side bar charts

WITH loan_sequence AS (
    SELECT 
        user_id,
        loan_id,
        revenue_total,
        cac,
        is_delinquent,
        risk_segment,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursed_date) as loan_number
    FROM main.fct_loan_financials
    WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
)
SELECT 
    risk_segment,
    CASE WHEN loan_number = 1 THEN 'Primer Prestamo' ELSE 'Recurrente' END as tipo_cliente,
    COUNT(*) as total_loans,
    COUNT(DISTINCT user_id) as customers,
    ROUND(AVG(revenue_total), 0) as avg_revenue,
    ROUND(SUM(CASE WHEN is_delinquent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as delinq_pct,
    ROUND(SUM(cac), 0) as total_cac
FROM loan_sequence
GROUP BY risk_segment, tipo_cliente
ORDER BY risk_segment, tipo_cliente DESC;


-- ============================================================================
-- QUERY 7: CAC ANALYSIS - SOLO PRESTAMOS #1
-- ============================================================================
-- Usar: Bar chart, box plot

WITH first_loans AS (
    SELECT 
        risk_segment,
        risk_band_production,
        cac
    FROM (
        SELECT 
            risk_segment,
            risk_band_production,
            cac,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursed_date) as loan_num
        FROM main.fct_loan_financials
        WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    )
    WHERE loan_num = 1 AND cac > 0
)
SELECT 
    risk_segment,
    COUNT(*) as customers_with_cac,
    ROUND(MIN(cac), 2) as min_cac,
    ROUND(AVG(cac), 2) as avg_cac,
    ROUND(MEDIAN(cac), 2) as median_cac,
    ROUND(MAX(cac), 2) as max_cac,
    ROUND(STDDEV(cac), 2) as stddev_cac
FROM first_loans
GROUP BY risk_segment
ORDER BY avg_cac;


-- ============================================================================
-- QUERY 8: CONTRIBUTION MARGIN ANALYSIS
-- ============================================================================
-- Usar: Bar chart, waterfall chart

SELECT 
    risk_segment,
    COUNT(*) as total_loans,
    ROUND(SUM(revenue_total), 0) as total_revenue,
    ROUND(SUM(funding_cost), 0) as total_funding_cost,
    ROUND(SUM(cogs), 0) as total_cogs,
    ROUND(SUM(cac), 0) as total_cac,
    ROUND(SUM(financial_margin), 0) as total_financial_margin,
    ROUND(SUM(contribution_margin), 0) as total_contribution_margin,
    ROUND(AVG(financial_margin_pct) * 100, 2) as avg_financial_margin_pct,
    ROUND(AVG(contribution_margin_pct) * 100, 2) as avg_contribution_margin_pct
FROM main.fct_loan_financials
WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
GROUP BY risk_segment
ORDER BY total_contribution_margin DESC;


-- ============================================================================
-- QUERY 9: TIME SERIES - MONTHLY TRENDS
-- ============================================================================
-- Usar: Line chart temporal

SELECT 
    DATE_TRUNC('month', vintage_month) as month,
    risk_segment,
    COUNT(*) as loans,
    COUNT(DISTINCT user_id) as customers,
    ROUND(SUM(revenue_total), 0) as revenue,
    ROUND(SUM(cac), 0) as cac,
    ROUND(SUM(revenue_total) / NULLIF(SUM(cac), 0), 2) as revenue_to_cac,
    ROUND(AVG(CASE WHEN is_delinquent = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) as delinq_pct
FROM main.fct_loan_financials
WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
GROUP BY month, risk_segment
ORDER BY month, risk_segment;


-- ============================================================================
-- QUERY 10: COHORT RETENTION - REPEAT CUSTOMERS
-- ============================================================================
-- Usar: Cohort retention chart

WITH customer_loans AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(disbursed_date)) as first_month,
        DATE_TRUNC('month', disbursed_date) as loan_month,
        COUNT(*) as loans_in_month
    FROM main.fct_loan_financials
    GROUP BY user_id, loan_month
)
SELECT 
    first_month as cohort,
    loan_month,
    COUNT(DISTINCT user_id) as active_customers,
    SUM(loans_in_month) as loans
FROM customer_loans
GROUP BY first_month, loan_month
ORDER BY first_month, loan_month;


-- ============================================================================
-- QUERY 11: DELINQUENCY DRILL-DOWN
-- ============================================================================
-- Usar: Stacked bar chart, treemap

SELECT 
    risk_segment,
    dpd_bucket,
    delinquency_status,
    COUNT(*) as loans,
    ROUND(SUM(revenue_total), 0) as revenue,
    ROUND(SUM(capital_balance), 0) as capital_at_risk,
    ROUND(AVG(loss_rate) * 100, 3) as avg_loss_rate_pct
FROM main.fct_loan_financials
WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
GROUP BY risk_segment, dpd_bucket, delinquency_status
ORDER BY risk_segment, dpd_bucket;


-- ============================================================================
-- QUERY 12: GEOGRAFIA - STATE PERFORMANCE
-- ============================================================================
-- Usar: Map visualization

WITH customer_lifetime AS (
    SELECT 
        l.user_id,
        MAX(l.state) as state,
        MAX(l.risk_segment) as risk_segment,
        SUM(l.revenue_total) as lifetime_revenue,
        SUM(l.cac) as total_cac
    FROM main.fct_loan_financials l
    WHERE l.risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    GROUP BY l.user_id
)
SELECT 
    state,
    COUNT(DISTINCT user_id) as customers,
    ROUND(SUM(lifetime_revenue), 0) as revenue,
    ROUND(SUM(total_cac), 0) as cac,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac), 0), 2) as ltv_cac,
    ROUND(SUM(lifetime_revenue) / COUNT(DISTINCT user_id), 0) as avg_ltv
FROM customer_lifetime
GROUP BY state
ORDER BY revenue DESC;

