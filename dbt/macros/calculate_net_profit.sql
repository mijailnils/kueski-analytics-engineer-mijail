{% macro calculate_net_profit(revenue, cost_of_funds, credit_loss, cogs, cac) %}
    /*
    Calcula la utilidad neta (net profit)
    
    Formula:
        Net Profit = Revenue - Cost of Funds - Credit Loss - COGS - CAC
        
        O equivalentemente:
        Net Profit = Contribution Margin - CAC
    
    Args:
        revenue: Ingresos totales del préstamo
        cost_of_funds: Costo de capital (TIIE-based)
        credit_loss: Pérdidas por defaults (charge-offs)
        cogs: Costos operativos (servicing, cobranza, etc.)
        cac: Customer acquisition cost (solo primer préstamo)
    
    Example:
        Revenue: $5,000
        Cost of Funds: $378
        Credit Loss: $2,000
        COGS: $150
        CAC: $100 (solo primer préstamo)
        Net Profit = 5,000 - 378 - 2,000 - 150 - 100 = $2,372
    
    Note:
        Este es el margen FINAL después de TODOS los costos.
        CAC solo aplica al primer préstamo del cliente.
        
    Margin waterfall completo:
        Revenue
        - Cost of Funds
        - Credit Loss
        = Financial Margin
        - COGS
        = Contribution Margin
        - CAC
        = Net Profit 
    */
    ({{ revenue }} - {{ cost_of_funds }} - {{ credit_loss }} - {{ cogs }} - {{ cac }})
{% endmacro %}
