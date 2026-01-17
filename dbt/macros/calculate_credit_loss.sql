{% macro calculate_credit_loss(charge_off_amount) %}
    coalesce({{ charge_off_amount }}, 0)
{% endmacro %}

    /*
    Calcula la pérdida por crédito (Credit Loss / Charge-offs)
    
    Este es el monto que se pierde cuando un cliente no paga (default).
    
    Args:
        charge_off_amount: Monto castigado por default
    
    Formula:
        Credit Loss = Charge-off Amount
    
    Example:
        Cliente defaulteó: $2,000
        Credit Loss = $2,000
    
    Note:
        Este concepto es SEPARADO del Cost of Funds.
        Ambos impactan la rentabilidad pero son costos diferentes:
        - Cost of Funds: costo de obtener capital
        - Credit Loss: pérdida por defaults
    */
