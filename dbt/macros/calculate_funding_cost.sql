{% macro calculate_funding_cost(loan_amount, loan_term, funding_rate_monthly) %}
    /*
    Calcula el costo de fondeo de capital (Cost of Funds)
    
    Este es el interés que Kueski paga por obtener el capital que luego presta.
    Basado en TIIE 28 días + spread.
    
    Args:
        loan_amount: Monto del préstamo
        loan_term: Plazo en meses
        funding_rate_monthly: Tasa mensual de funding cost (TIIE + spread)
    
    Formula:
        Cost of Funds = Loan Amount × Funding Rate × Term
    
    Example:
        Loan: $10,000
        Term: 3 meses
        Rate: 1.26% mensual (0.0126)
        Cost of Funds = 10,000 × 0.0126 × 3 = $378
    
    Note:
        NO incluye:
        - Credit losses (charge-offs)
        - COGS (costos operativos)
        - CAC (customer acquisition)
    */
    ({{ loan_amount }} * {{ funding_rate_monthly }} * {{ loan_term }})
{% endmacro %}
