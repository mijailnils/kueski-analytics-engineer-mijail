{% macro calculate_financial_margin(revenue, cost_of_funds, credit_loss) %}
    /*
    Calcula el margen financiero
    
    Formula:
        Financial Margin = Revenue - Cost of Funds - Credit Loss
    
    Args:
        revenue: Ingresos totales del préstamo
        cost_of_funds: Costo de capital (TIIE-based)
        credit_loss: Pérdidas por defaults (charge-offs)
    
    Example:
        Revenue: $5,000
        Cost of Funds: $378
        Credit Loss: $2,000
        Financial Margin = 5,000 - 378 - 2,000 = $2,622
    
    Note:
        Este es el margen DESPUÉS de costos de capital y pérdidas crediticias,
        pero ANTES de costos operativos (COGS) y adquisición (CAC).
    */
    ({{ revenue }} - {{ cost_of_funds }} - {{ credit_loss }})
{% endmacro %}
