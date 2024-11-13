# Prorrateo ERNC
Aplicación para reasignar el punto de operación para las centrales con CV=0 (se adapta hasta CV=0.1), ante situaciones de exceso de energía (curtailment).

## Instalación.
Utilizar UV para instalar la aplicación como CLI.

## Uso.
Agregar al archivo de post-proceso.bat -> prorrata %SOLUTION_0%

## TODO.
1. Agregar módulo por subzonas.
2. Agregar módulo con calculo de mínimos técnicos.
3. Agregar módulo para considerar centrales en horas sin SSCC.
