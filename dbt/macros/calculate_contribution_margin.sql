{% macro calculate_contribution_margin(revenue, cost_of_funds, credit_loss, cogs) %}
    /*
    Calcula el margen de contribución
    
    Formula:
        Contribution Margin = Revenue - Cost of Funds - Credit Loss - COGS
        
        O equivalentemente:
        Contribution Margin = Financial Margin - COGS
    
    Args:
        revenue: Ingresos totales del préstamo
        cost_of_funds: Costo de capital (TIIE-based)
        credit_loss: Pérdidas por defaults (charge-offs)
        cogs: Costos operativos (servicing, cobranza, etc.)
    
    Example:
        Revenue: $5,000
        Cost of Funds: $378
        Credit Loss: $2,000
        COGS: $150
        Contribution Margin = 5,000 - 378 - 2,000 - 150 = $2,472
    
    Note:
        Este es el margen DESPUÉS de todos los costos variables,
        pero ANTES de CAC (customer acquisition cost).
    */
    ({{ revenue }} - {{ cost_of_funds }} - {{ credit_loss }} - {{ cogs }})
{% endmacro %}