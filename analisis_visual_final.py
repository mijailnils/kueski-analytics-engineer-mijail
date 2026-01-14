# -*- coding: utf-8 -*-
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

# Configuracion
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
Path('analisis_adhoc/graficos_finales').mkdir(parents=True, exist_ok=True)

conn = duckdb.connect('dbt/kueski_finance.duckdb')

print("="*80)
print("ANALISIS VISUAL COMPLETO - Q1 2025 PORTFOLIO")
print("="*80)

# GRAFICO 1: REVENUE/CAC POR SEGMENTO (CORREGIDO)
print("\nGenerando Grafico 1: Revenue/CAC por Segmento...")

query1 = """
WITH customer_lifetime AS (
    SELECT 
        l.user_id,
        MAX(l.risk_segment) as risk_segment,
        SUM(l.revenue_total) as lifetime_revenue,
        SUM(l.cac) as total_cac_paid
    FROM main.fct_loan_financials l
    WHERE l.risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    GROUP BY l.user_id
)
SELECT 
    risk_segment,
    COUNT(DISTINCT user_id) as customers,
    ROUND(SUM(lifetime_revenue), 0) as total_revenue,
    ROUND(SUM(total_cac_paid), 0) as total_cac,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac_paid), 0), 2) as ltv_cac
FROM customer_lifetime
GROUP BY risk_segment
ORDER BY total_revenue DESC
"""

df1 = conn.execute(query1).df()

fig, ax = plt.subplots(figsize=(10, 6))
colors = ['#2ecc71' if x >= 5 else '#e74c3c' if x < 3 else '#f39c12' 
          for x in df1['ltv_cac']]
bars = ax.barh(df1['risk_segment'], df1['ltv_cac'], color=colors)
ax.axvline(x=3, color='red', linestyle='--', linewidth=2, label='Minimo 3x')
ax.set_xlabel('LTV/CAC Ratio', fontsize=12, fontweight='bold')
ax.set_title('LTV/CAC por Segmento de Riesgo (CORREGIDO)', 
             fontsize=14, fontweight='bold')
ax.legend()

for i, (idx, row) in enumerate(df1.iterrows()):
    ax.text(row['ltv_cac'] + 0.2, i, f"{row['ltv_cac']:.2f}x", 
            va='center', fontweight='bold', fontsize=11)

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/01_ltv_cac_por_segmento.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 01_ltv_cac_por_segmento.png")

# GRAFICO 2: PORTFOLIO MIX
print("\nGenerando Grafico 2: Portfolio Mix...")

query2 = """
SELECT 
    risk_segment,
    COUNT(*) as loans,
    SUM(revenue_total) as revenue
FROM main.fct_loan_financials
WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
GROUP BY risk_segment
"""

df2 = conn.execute(query2).df()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

colors_pie = ['#e74c3c', '#f39c12', '#2ecc71']
ax1.pie(df2['loans'], labels=df2['risk_segment'], autopct='%1.1f%%',
        startangle=90, colors=colors_pie)
ax1.set_title('Portfolio Mix - Volumen de Prestamos', fontweight='bold')

ax2.pie(df2['revenue'], labels=df2['risk_segment'], autopct='%1.1f%%',
        startangle=90, colors=colors_pie)
ax2.set_title('Portfolio Mix - Revenue', fontweight='bold')

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/02_portfolio_mix.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 02_portfolio_mix.png")

# GRAFICO 3: VINTAGE CURVES
print("\nGenerando Grafico 3: Vintage Curves...")

query3 = """
SELECT 
    vintage_month,
    months_on_book,
    delinquency_rate
FROM main.fct_vintage_curves
WHERE vintage_month IN ('2025-01-01', '2025-02-01', '2025-03-01')
ORDER BY vintage_month, months_on_book
"""

df3 = conn.execute(query3).df()

fig, ax = plt.subplots(figsize=(12, 7))

for vintage in df3['vintage_month'].unique():
    data = df3[df3['vintage_month'] == vintage]
    label = pd.to_datetime(vintage).strftime('%b %Y')
    ax.plot(data['months_on_book'], 
            data['delinquency_rate'] * 100,
            marker='o', linewidth=2, label=label)

ax.set_xlabel('Meses desde Desembolso', fontsize=12, fontweight='bold')
ax.set_ylabel('Delinquency Rate (%)', fontsize=12, fontweight='bold')
ax.set_title('Vintage Curves: Evolucion de Delinquency por Cohorte', 
             fontsize=14, fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/03_vintage_curves.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 03_vintage_curves.png")

# GRAFICO 4: CAC POR SEGMENTO
print("\nGenerando Grafico 4: CAC por Segmento...")

query4 = """
WITH first_loans AS (
    SELECT 
        risk_segment,
        cac
    FROM (
        SELECT 
            risk_segment,
            cac,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursed_date) as loan_num
        FROM main.fct_loan_financials
        WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    )
    WHERE loan_num = 1 AND cac > 0
)
SELECT 
    risk_segment,
    COUNT(*) as customers,
    ROUND(AVG(cac), 2) as avg_cac
FROM first_loans
GROUP BY risk_segment
"""

df4 = conn.execute(query4).df()

fig, ax = plt.subplots(figsize=(10, 6))
colors = ['#2ecc71', '#f39c12', '#e74c3c']
bars = ax.bar(df4['risk_segment'], df4['avg_cac'], color=colors)
ax.set_ylabel('CAC Promedio ($)', fontsize=12, fontweight='bold')
ax.set_title('CAC Promedio por Segmento (Solo Prestamo #1)', 
             fontsize=14, fontweight='bold')

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'${height:.0f}',
            ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/04_cac_por_segmento.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 04_cac_por_segmento.png")

# GRAFICO 5: PERFORMANCE POR RISK BAND
print("\nGenerando Grafico 5: Performance por Risk Band...")

query5 = """
WITH customer_lifetime AS (
    SELECT 
        user_id,
        MAX(risk_band_production) as risk_band,
        SUM(revenue_total) as lifetime_revenue,
        SUM(cac) as total_cac,
        AVG(CASE WHEN is_delinquent = 1 THEN 1.0 ELSE 0.0 END) as delinq_rate
    FROM main.fct_loan_financials
    WHERE risk_band_production IN ('1', '2', '3', '4.1', '4.2', '5')
    GROUP BY user_id
)
SELECT 
    risk_band,
    COUNT(*) as customers,
    ROUND(AVG(lifetime_revenue), 0) as avg_revenue,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac), 0), 2) as ltv_cac,
    ROUND(AVG(delinq_rate) * 100, 1) as avg_delinq_pct
FROM customer_lifetime
GROUP BY risk_band
ORDER BY 
    CASE 
        WHEN risk_band = '1' THEN 1
        WHEN risk_band = '2' THEN 2
        WHEN risk_band = '3' THEN 3
        WHEN risk_band = '4.1' THEN 4
        WHEN risk_band = '4.2' THEN 5
        WHEN risk_band = '5' THEN 6
    END
"""

df5 = conn.execute(query5).df()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

ax1.bar(df5['risk_band'], df5['ltv_cac'], 
        color=['#e74c3c' if x < 3 else '#f39c12' if x < 5 else '#2ecc71' 
               for x in df5['ltv_cac']])
ax1.axhline(y=3, color='red', linestyle='--', linewidth=2)
ax1.set_xlabel('Risk Band', fontweight='bold')
ax1.set_ylabel('LTV/CAC', fontweight='bold')
ax1.set_title('LTV/CAC por Risk Band', fontweight='bold')

ax2.bar(df5['risk_band'], df5['avg_delinq_pct'], color='#3498db')
ax2.set_xlabel('Risk Band', fontweight='bold')
ax2.set_ylabel('Delinquency (%)', fontweight='bold')
ax2.set_title('Delinquency Promedio por Risk Band', fontweight='bold')

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/05_performance_risk_band.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 05_performance_risk_band.png")

# GRAFICO 6: CLIENTES RECURRENTES
print("\nGenerando Grafico 6: Analisis Recurrentes...")

query6 = """
WITH loan_sequence AS (
    SELECT 
        user_id,
        revenue_total,
        is_delinquent,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY disbursed_date) as loan_num
    FROM main.fct_loan_financials
)
SELECT 
    CASE WHEN loan_num = 1 THEN 'Primer Prestamo' ELSE 'Recurrente' END as tipo,
    COUNT(*) as loans,
    ROUND(AVG(revenue_total), 0) as avg_revenue,
    ROUND(SUM(CASE WHEN is_delinquent = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as delinq_pct
FROM loan_sequence
GROUP BY tipo
ORDER BY tipo DESC
"""

df6 = conn.execute(query6).df()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

ax1.bar(df6['tipo'], df6['loans'], color=['#3498db', '#2ecc71'])
ax1.set_ylabel('Numero de Prestamos', fontweight='bold')
ax1.set_title('Volumen: Primer Prestamo vs Recurrente', fontweight='bold')
for i, v in enumerate(df6['loans']):
    ax1.text(i, v, f'{v:,}', ha='center', va='bottom', fontweight='bold')

ax2.bar(df6['tipo'], df6['delinq_pct'], color=['#e74c3c', '#2ecc71'])
ax2.set_ylabel('Delinquency (%)', fontweight='bold')
ax2.set_title('Delinquency: Primer vs Recurrente', fontweight='bold')
for i, v in enumerate(df6['delinq_pct']):
    ax2.text(i, v, f'{v}%', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/06_primer_vs_recurrente.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 06_primer_vs_recurrente.png")

# GRAFICO 7: HEATMAP
print("\nGenerando Grafico 7: Heatmap Vintage x Segmento...")

query7 = """
WITH customer_lifetime AS (
    SELECT 
        l.user_id,
        DATE_TRUNC('month', l.disbursed_date) as vintage,
        MAX(l.risk_segment) as risk_segment,
        SUM(l.revenue_total) as lifetime_revenue,
        SUM(l.cac) as total_cac
    FROM main.fct_loan_financials l
    WHERE l.risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
    GROUP BY l.user_id, vintage
)
SELECT 
    strftime(vintage, '%b') as vintage_month,
    risk_segment,
    ROUND(SUM(lifetime_revenue) / NULLIF(SUM(total_cac), 0), 2) as ltv_cac
FROM customer_lifetime
GROUP BY vintage, risk_segment
ORDER BY vintage, risk_segment
"""

df7 = conn.execute(query7).df()
pivot = df7.pivot(index='risk_segment', columns='vintage_month', values='ltv_cac')

fig, ax = plt.subplots(figsize=(10, 6))
sns.heatmap(pivot, annot=True, fmt='.2f', cmap='RdYlGn', center=5,
            cbar_kws={'label': 'LTV/CAC'}, ax=ax, vmin=0, vmax=12)
ax.set_title('Heatmap: LTV/CAC por Vintage x Segmento', 
             fontsize=14, fontweight='bold')
ax.set_xlabel('Mes de Vintage', fontweight='bold')
ax.set_ylabel('Segmento de Riesgo', fontweight='bold')

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/07_heatmap_vintage_segment.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 07_heatmap_vintage_segment.png")

# GRAFICO 8: CONTRIBUTION MARGIN
print("\nGenerando Grafico 8: Contribution Margin...")

query8 = """
SELECT 
    risk_segment,
    ROUND(AVG(contribution_margin_pct) * 100, 1) as avg_margin_pct,
    COUNT(*) as loans
FROM main.fct_loan_financials
WHERE risk_segment IN ('High Risk', 'Medium Risk', 'Low Risk')
GROUP BY risk_segment
ORDER BY avg_margin_pct DESC
"""

df8 = conn.execute(query8).df()

fig, ax = plt.subplots(figsize=(10, 6))
colors = ['#2ecc71' if x > 99 else '#f39c12' for x in df8['avg_margin_pct']]
bars = ax.bar(df8['risk_segment'], df8['avg_margin_pct'], color=colors)
ax.set_ylabel('Contribution Margin (%)', fontsize=12, fontweight='bold')
ax.set_title('Contribution Margin Promedio por Segmento', 
             fontsize=14, fontweight='bold')
ax.set_ylim(90, 100)

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}%',
            ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('analisis_adhoc/graficos_finales/08_contribution_margin.png', 
            dpi=300, bbox_inches='tight')
plt.close()
print("OK Guardado: 08_contribution_margin.png")

conn.close()

print("\n" + "="*80)
print("ANALISIS COMPLETO - 8 GRAFICOS GENERADOS")
print("="*80)
print(f"\nUbicacion: analisis_adhoc/graficos_finales/")
