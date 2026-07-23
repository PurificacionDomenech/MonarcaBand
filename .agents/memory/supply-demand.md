---
name: Supply/Demand replaces FVG
description: Zonas de oferta/demanda basadas en pivots de precio, con +1 punto extra por creación/tacto en velas adyacentes
---

## Reglas de Supply/Demand

**Detección**: Pivots de precio (lookback=10) con zona ±ATR*0.15
- **Supply zone** (bajista): creada en pivot high → rechazo alcista
- **Demand zone** (alcista): creada en pivot low → rechazo bajista

**Scoring**:
- Zona activa (touched, no rota): **+2 pts**
- Zona creada en vela anterior: **+1 pt extra** (dirección CONTRARIA a la zona)
- Zona tocada en vela actual: **+1 pt extra** (dirección CONTRARIA = rebote)

**Dirección de los puntos extra**:
- Demand (bull) creada/tocada → señal **bearish** (+1 pt de rebote)
- Supply (bear) creada/tocada → señal **bullish** (+1 pt de rebote)

**Por qué**: Una zona de demanda indica que compradores defendieron ese nivel → el siguiente toque/rebote sugiere que venderán (dirección contraria). Igual para supply.

**Estados de zona**:
- `strong`: recién creada, sin tocar
- `touched`: precio entró en zona pero no la rompió
- `broken`: Close rompió la zona (invalidada, no cuenta)

**Ventana de datos**: 3 meses para detectar zonas históricas (antes 1 mes, insuficiente).

**Why**: El indicador Pine Script original de Pury Santana detecta zonas de liquidez basadas en pivots con ATR*0.15 de cuerpo. Esto es conceptualmente diferente de FVG (huecos entre velas), y alinea mejor con la estrategia de rechazo de niveles.
