import re

# Leer el notebook
with open('05_analisis_completo_visual.ipynb', 'r', encoding='utf-8') as f:
    content = f.read()

# Nuevo c√≥digo corregido
new_code = '''# Calcular mejora (con validaci√≥n)
if len(df_vintage) > 0:
    jan_data = df_vintage[df_vintage['vintage_month'] == '2025-01-01']
    mar_data = df_vintage[df_vintage['vintage_month'] == '2025-03-01']
    
    if len(jan_data) > 0 and len(mar_data) > 0:
        jan_final = jan_data['delinquency_rate'].iloc[-1]
        mar_final = mar_data['delinquency_rate'].iloc[-1]
        mejora = ((mar_final - jan_final) / jan_final) * 100
        print(f"\\nÌ≥à Mejora de Ene a Mar: {mejora:.1f}%")
    else:
        print("\\n‚ö†Ô∏è No hay suficientes datos para calcular mejora")
else:
    print("\\n‚ö†Ô∏è No hay datos de vintage curves")'''

# Reemplazar el c√≥digo problem√°tico
old_pattern = r'# Calcular mejora\njan_final.*?print\(f"\\nÌ≥à Mejora de Ene a Mar: {mejora:.1f}%"\)'
content = re.sub(old_pattern, new_code, content, flags=re.DOTALL)

# Guardar
with open('05_analisis_completo_visual.ipynb', 'w', encoding='utf-8') as f:
    f.write(content)

print("‚úÖ Notebook corregido")
